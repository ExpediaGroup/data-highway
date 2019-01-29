/**
 * Copyright (C) 2016-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.road.rest.app;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.springframework.boot.Banner.Mode.OFF;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.http.RequestEntity.get;
import static org.springframework.http.RequestEntity.post;
import static org.springframework.http.RequestEntity.put;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.userdetails.User;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.kafkastore.exceptions.SerializationException;
import com.hotels.road.kafkastore.serialization.Serializer;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.onramp.app.OnrampApp;
import com.hotels.road.paver.app.PaverApp;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Onramp;
import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.rest.model.RoadModel;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.schema.serde.SchemaSerializationModule;
import com.hotels.road.security.RoadWebSecurityConfigurerAdapter;
import com.hotels.road.tollbooth.client.api.Operation;
import com.hotels.road.tollbooth.client.api.PatchOperation;

public class RoadEndpointsIntegrationTest {

  private static final String SCHEMAS_TOPIC = "_schemas";
  private static final String ROADS_TOPIC = "_roads";
  private static final int NUM_BROKERS = 1;
  private static final AtomicLong sequence = new AtomicLong(0);

  @ClassRule
  public static final EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS);

  private static String patchTopic;
  private static String baseUri;
  private static ConfigurableApplicationContext context;
  private static KafkaConsumer<String, String> patchConsumer;

  private static final String TOPIC_PREFIX = "topic.";
  private final RestTemplate rest = newRestTemplate();

  private final Schema schema1 = SchemaBuilder
      .record("record")
      .fields()
      .name("field")
      .type()
      .booleanType()
      .noDefault()
      .endRecord();

  private final Schema schema2 = SchemaBuilder
      .record("record")
      .fields()
      .name("field")
      .type()
      .booleanType()
      .booleanDefault(true)
      .endRecord();

  @BeforeClass
  public static void before() throws IOException, InterruptedException {
    kafkaCluster.createTopic(SCHEMAS_TOPIC, 1, 1);
    Properties topicConfig = new Properties();
    topicConfig.setProperty("cleanup.policy", "compact");
    kafkaCluster.createTopic(ROADS_TOPIC, 1, 1, topicConfig);

    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    patchTopic = "_road_patch_" + sequence.incrementAndGet();
    kafkaCluster.createTopic(patchTopic, 1, 1);

    Map<String, Object> properties = ImmutableMap
        .<String, Object> builder()
        .put("server.port", port)
        .put("INSTANCE_NAME", "road-rest")
        .put("kafka.road.zookeeper", kafkaCluster.zKConnectString())
        .put("kafka.bootstrapServers", SecurityProtocol.PLAINTEXT + "://" + kafkaCluster.bootstrapServers())
        .put("kafka.road.default.replicationFactor", 1)
        .put("kafka.road.replicas", 1)
        .put("kafka.road.topic", "_roads")
        .put("kafka.road.modification.topic", patchTopic)
        .put("paver.topic.prefix", TOPIC_PREFIX)
        .put("paver.authorisation.cidr-blocks", "0.0.0.0/0")
        .put("paver.authorisation.authorities", "ADMIN_ROLE")
        .put("notification.sns.region", "us-west-2")
        .build();

    context = new SpringApplicationBuilder(PaverApp.class, OnrampApp.class, TestConfiguration.class)
        .bannerMode(OFF)
        .properties(properties)
        .build()
        .run();

    baseUri = String.format("http://localhost:%s", port);

    patchConsumer = createPatchConsumer();
  }

  @Configuration
  @EnableGlobalMethodSecurity(prePostEnabled = true)
  public static class TestConfiguration {
    @Bean
    @Primary
    public com.amazonaws.services.sns.AmazonSNSAsync amazonSnsAsync() {
      return mock(com.amazonaws.services.sns.AmazonSNSAsync.class);
    }

    @Bean
    public WebSecurityConfigurerAdapter webSecurityConfigurerAdapter() {
      return new RoadWebSecurityConfigurerAdapter() {
        @SuppressWarnings("deprecation")
        @Override
        protected void configure(AuthenticationManagerBuilder auth) throws Exception {
          auth.inMemoryAuthentication().withUser(
              User.withDefaultPasswordEncoder().username("user").password("pass").authorities("ROLE_USER"));
        }
      };
    }
  }

  @AfterClass
  public static void after() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void createRoadAndSchemas() throws Exception {
    // create Road
    String roadName = "a1";
    BasicRoadModel basicRoadModel = new BasicRoadModel(roadName, "a1 road", "a1 team", "a1@example.org", true, "", null,
        Maps.newHashMap());
    ResponseEntity<StandardResponse> createRoadResult = rest.exchange(post(uri("/paver/v1/roads"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body(basicRoadModel), StandardResponse.class);
    assertThat(createRoadResult.getStatusCode(), is(HttpStatus.OK));
    assertThat(createRoadResult.getBody().getMessage(), is("Request to create road \"a1\" received"));

    Awaitility.await().atMost(Duration.FIVE_SECONDS).until(() -> {
      String expected = "{\"documentId\":\"a1\",\"operations\":[{\"op\":\"add\",\"path\":\"\",\"value\":{\"name\":\"a1\",\"topicName\":null,\"description\":\"a1 road\",\"teamName\":\"a1 team\",\"contactEmail\":\"a1@example.org\",\"enabled\":true,\"partitionPath\":\"\",\"metadata\":{},\"schemas\":{},\"destinations\":{},\"status\":null,\"compatibilityMode\":\"CAN_READ_ALL\"}}]}";
      String actual = readRecords(patchConsumer, 1).get(0);
      assertJsonEquals(expected, actual);
    });

    // Emulate TollBooth to process the above patch and create road for us.
    Map<String, Road> kafkaStore = kafkaStore();
    Road road = new Road();
    road.setName(roadName);
    kafkaStore.put(roadName, road);

    // Add schema to road.
    ResponseEntity<StandardResponse> schemaResult = rest.exchange(post(uri("/paver/v1/roads/a1/schemas"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body(schema1.toString()), StandardResponse.class);
    assertThat(schemaResult.getStatusCode(), is(HttpStatus.OK));
    assertThat(schemaResult.getBody().getMessage(), is("Request to add a new schema received."));

    Awaitility.await().atMost(Duration.FIVE_SECONDS).until(() -> {
      List<String> records = readRecords(patchConsumer, 1);
      String expectedPatch = "{\"documentId\":\"a1\",\"operations\":[{\"op\":\"add\",\"path\":\"/schemas/1\",\"value\":{\"schema\":{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"field\",\"type\":\"boolean\"}]},\"version\":1,\"deleted\":false}}]}";
      assertJsonEquals(expectedPatch, records.get(0));
    });
  }

  @Test
  public void getRoad() throws Exception {
    String roadName = "theroad";
    createRoadWithSchema(roadName, true, new SchemaVersion(schema1, 1, false));
    ResponseEntity<RoadModel> roadResult = rest.exchange(get(uri("/paver/v1/roads/" + roadName)).build(),
        RoadModel.class);
    assertThat(roadResult.getStatusCode(), is(HttpStatus.OK));
    assertThat(roadResult.getBody().getName(), is(roadName));
  }

  @Test
  public void updateRoadMetadata() throws Exception {
    String roadName = "updated_road";
    List<PatchOperation> patchOperations = new ArrayList<>();
    patchOperations.add(new PatchOperation(Operation.REPLACE, "/contactEmail", "a1@example.org"));
    patchOperations.add(new PatchOperation(Operation.REPLACE, "/description", "desc"));
    patchOperations.add(new PatchOperation(Operation.REPLACE, "/teamName", "team a1"));

    Road road = new Road();
    road.setName(roadName);
    road.setEnabled(true);
    road.setDescription("description2");

    KafkaStore<String, Road> kafkaStore = kafkaStore();
    kafkaStore.put(roadName, road);

    MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
    headers.add("Content-Type", "application/json");

    rest.exchange(put(uri("/paver/v1/roads/updated_road"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .header("Content-Type", "application/json-patch+json")
        .body(patchOperations), StandardResponse.class);

    Awaitility.await().atMost(Duration.FIVE_SECONDS).until(() -> {
      String expected = "{\"documentId\":\"updated_road\",\"operations\":[{\"op\":\"replace\",\"path\":\"/contactEmail\",\"value\":\"a1@example.org\"},{\"op\":\"replace\",\"path\":\"/description\",\"value\":\"desc\"},{\"op\":\"replace\",\"path\":\"/teamName\",\"value\":\"team a1\"}]}";
      String actual = readRecords(patchConsumer, 1).get(0);
      assertJsonEquals(expected, actual);
    });

  }

  @Test
  public void getSchema() throws Exception {
    String roadName = "schemaroad";
    createRoadWithSchema(roadName, true, new SchemaVersion(schema1, 1, false));

    ResponseEntity<String> schemaResult = rest.exchange(get(uri("/paver/v1/roads/" + roadName + "/schemas/1")).build(),
        String.class);
    assertThat(schemaResult.getStatusCode(), is(HttpStatus.OK));
    assertThat(schemaResult.getBody(), is(schema1.toString()));
  }

  @Test
  public void getLatestSchemaForNonExistentRoad() throws Exception {
    ResponseEntity<String> schemaResult = rest.exchange(get(uri("/paver/v1/roads/road1/schemas/latest")).build(),
        String.class);

    assertThat(schemaResult.getStatusCode(), is(HttpStatus.NOT_FOUND));
    assertThat(schemaResult.getBody(), containsString("Road \\\"road1\\\" does not exist."));
  }

  @Test
  public void updateRoadSchema() throws Exception {
    // Create Road with schema
    String roadName = "metaroad";
    createRoadWithSchema(roadName, true, new SchemaVersion(schema1, 1, false));

    // Add another schema to road.
    ResponseEntity<StandardResponse> schemaResult = rest.exchange(post(uri("/paver/v1/roads/metaroad/schemas"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body(schema2.toString()), StandardResponse.class);

    assertThat(schemaResult.getStatusCode(), is(HttpStatus.OK));
    assertThat(schemaResult.getBody().getMessage(), is("Request to add a new schema received."));

    Awaitility.await().atMost(Duration.FIVE_SECONDS).until(() -> {
      String expectedPatch = "{\"documentId\":\"metaroad\",\"operations\":[{\"op\":\"add\",\"path\":\"/schemas/2\",\"value\":{\"schema\":{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"field\",\"type\":\"boolean\"}]},\"version\":2,\"deleted\":false}}]}";
      assertJsonEquals(expectedPatch, readRecords(patchConsumer, 1).get(0));
    });

  }

  @Test
  public void nonCompatibleSchema() throws Exception {
    String roadName = "noncompatibleroad";
    kafkaCluster.createTopic(roadName, 1, 1);
    createRoadWithSchema(roadName, true, new SchemaVersion(schema1, 1, false));
    Schema nonCompatibleSchema = SchemaBuilder
        .record("record")
        .fields()
        .name("field2")
        .type()
        .booleanType()
        .noDefault()
        .endRecord();

    ResponseEntity<StandardResponse> schemaResult = rest.exchange(post(uri("/paver/v1/roads/noncompatibleroad/schemas"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body(nonCompatibleSchema.toString()), StandardResponse.class);

    assertThat(schemaResult.getStatusCode(), is(HttpStatus.CONFLICT));
    assertThat(schemaResult.getBody().getMessage(), is(
        "Invalid schema. Compatibility type 'CAN_READ' does not hold between 1 schema(s) in the chronology because: Schema[0] has incompatibilities: ['READER_FIELD_MISSING_DEFAULT_VALUE: field2' at '/fields/0']."));

  }

  @Test
  public void addSchemaToNonExistentRoad() throws Exception {
    ResponseEntity<StandardResponse> schemaResult = rest.exchange(post(uri("/paver/v1/roads/nonexistentroad/schemas"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body(schema1.toString()), StandardResponse.class);

    assertThat(schemaResult.getStatusCode(), is(HttpStatus.NOT_FOUND));
    assertThat(schemaResult.getBody().getMessage(), is("Road \"nonexistentroad\" does not exist."));
  }

  @Test
  public void postEvent() throws Exception {
    String roadName = "post_road";
    createRoadWithSchema(roadName, true, new SchemaVersion(schema1, 1, false));
    kafkaCluster.createTopic(TOPIC_PREFIX + roadName, 1, 1);

    ResponseEntity<StandardResponse[]> result = rest.exchange(post(uri("/onramp/v1/roads/post_road/messages"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body("[{\"field\":true}]"), StandardResponse[].class);

    assertThat(result.getStatusCode(), is(HttpStatus.OK));
    StandardResponse[] body = result.getBody();
    assertThat(body.length, is(1));
    assertThat(body[0].isSuccess(), is(true));
    assertThat(body[0].getMessage(), is("Message accepted."));

  }

  @Test
  public void postEventGzip() throws Exception {
    String roadName = "gzipRoad";
    createRoadWithSchema(roadName, true, new SchemaVersion(schema1, 1, false));
    kafkaCluster.createTopic(TOPIC_PREFIX + roadName, 1, 1);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream gzip = new GZIPOutputStream(baos)) {
      gzip.write("[{\"field\":true}]".getBytes(UTF_8));
    }
    byte[] bytes = baos.toByteArray();

    ResponseEntity<StandardResponse[]> result = rest.exchange(post(uri("/onramp/v1/roads/gzipRoad/messages"))
        .header(CONTENT_ENCODING, "gzip")
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body(bytes), StandardResponse[].class);

    assertThat(result.getStatusCode(), is(HttpStatus.OK));
    StandardResponse[] body = result.getBody();
    assertThat(body.length, is(1));
    assertThat(body[0].isSuccess(), is(true));
    assertThat(body[0].getMessage(), is("Message accepted."));
  }

  @Test
  public void postEventRejected() throws Exception {
    String roadName = "rejecting_road";
    createRoadWithSchema(roadName, true, new SchemaVersion(schema1, 1, false));
    kafkaCluster.createTopic(TOPIC_PREFIX + roadName, 1, 1);

    ResponseEntity<StandardResponse[]> result = rest.exchange(post(uri("/onramp/v1/roads/rejecting_road/messages"))
        .header(AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(UTF_8)))
        .contentType(APPLICATION_JSON_UTF8)
        .body("[{\"field\":\"THIS_SHOULD_BE_BOOLEAN\"}]"), StandardResponse[].class);

    assertThat(result.getStatusCode(), is(HttpStatus.OK));
    StandardResponse[] body = result.getBody();
    assertThat(body.length, is(1));
    assertThat(body[0].isSuccess(), is(false));
    assertThat(body[0].getMessage(), is("The event failed validation. Cannot convert field field"));
  }

  private URI uri(String path) throws URISyntaxException {
    return new URI(baseUri + path);
  }

  private static RestTemplate newRestTemplate() {
    RestTemplate rest = new RestTemplate();

    rest.setErrorHandler(new ResponseErrorHandler() {
      @Override
      public boolean hasError(ClientHttpResponse response) throws IOException {
        return false;
      }

      @Override
      public void handleError(ClientHttpResponse response) throws IOException {}
    });

    return rest;
  }

  private void assertJsonEquals(String expected, String actual) {
    try {
      JSONAssert.assertEquals(expected, actual, false);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> readRecords(KafkaConsumer<String, String> consumer, int records) {
    List<String> result = new ArrayList<>(records);
    while (result.size() < records) {
      consumer.poll(100L).forEach(x -> result.add(x.value()));
    }
    return result;
  }

  private static KafkaConsumer<String, String> createPatchConsumer() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaCluster.bootstrapServers());
    properties.setProperty("group.id", UUID.randomUUID().toString());
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("enable.auto.commit", "false");
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties, new StringDeserializer(),
        new StringDeserializer());
    kafkaConsumer.subscribe(Lists.newArrayList(patchTopic));
    return kafkaConsumer;
  }

  private Map<String, Road> createRoadWithSchema(String name, boolean enabled, SchemaVersion schemaVersion) {
    KafkaStore<String, Road> store = kafkaStore();
    Road road = new Road();
    road.setName(name);
    road.setEnabled(enabled);
    road.setTopicName(TOPIC_PREFIX + name);
    Map<Integer, SchemaVersion> schemas = new HashMap<>();
    schemas.put(1, schemaVersion);
    road.setSchemas(schemas);
    Authorisation authorisation = new Authorisation();
    road.setAuthorisation(authorisation);
    Onramp onramp = new Onramp();
    authorisation.setOnramp(onramp);
    onramp.setAuthorities(Collections.singletonList("ROLE_USER"));
    onramp.setCidrBlocks(Collections.singletonList("0.0.0.0/0"));
    store.put(name, road);
    return store;
  }

  private KafkaStore<String, Road> kafkaStore() {
    return new KafkaStore<>(kafkaCluster.bootstrapServers(), new RoadSerializer(), ROADS_TOPIC);
  }

  static class RoadSerializer implements Serializer<String, Road> {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new SchemaSerializationModule());

    @Override
    public byte[] serializeKey(String key) throws SerializationException {
      return key.getBytes(UTF_8);
    }

    @Override
    public byte[] serializeValue(Road value) throws SerializationException {
      try {
        return mapper.writeValueAsBytes(value);
      } catch (JsonProcessingException e) {
        throw new SerializationException(e);
      }
    }

    @Override
    public String deserializeKey(byte[] key) throws SerializationException {
      return new String(key, UTF_8);
    }

    @Override
    public Road deserializeValue(byte[] value) throws SerializationException {
      try {
        return mapper.readValue(value, Road.class);
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    }
  }

}
