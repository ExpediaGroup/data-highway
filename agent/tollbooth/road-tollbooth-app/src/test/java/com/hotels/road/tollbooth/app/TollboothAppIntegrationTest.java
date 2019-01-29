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
package com.hotels.road.tollbooth.app;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.Banner.Mode.OFF;

import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.kafkastore.KafkaStore;

public class TollboothAppIntegrationTest {
  private static final String ROAD_TOPIC = "road";
  private static final String PATCH_TOPIC = "patch";

  private static final int NUM_BROKERS = 1;

  private static final ObjectMapper mapper = new ObjectMapper();

  @ClassRule
  public static EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(NUM_BROKERS);
  private static ConfigurableApplicationContext context;

  private static KafkaProducer<String, String> producer;

  private static Map<String, JsonNode> store;

  private static int port;

  @BeforeClass
  public static void beforeClass() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    context = new SpringApplicationBuilder(TollboothApp.class)
        .bannerMode(OFF)
        .properties(ImmutableMap
            .<String, Object> builder()
            .put("server.port", port)
            .put("kafka.bootstrapServers", kafka.bootstrapServers())
            .put("kafka.zookeeper", kafka.zKConnectString())
            .put("kafka.store.topic", ROAD_TOPIC)
            .put("kafka.store.replicas", "1")
            .put("kafka.patch.topic", PATCH_TOPIC)
            .put("kafka.patch.replicas", "1")
            .put("kafka.patch.groupId", "patches")
            .build())
        .build()
        .run();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafka.bootstrapServers());
    producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());

    store = new KafkaStore<>(kafka.bootstrapServers(), new JsonNodeSerializer(mapper), ROAD_TOPIC);
  }

  @Test(timeout = 20000)
  public void create_document_from_patch() throws Exception {
    producer.send(new ProducerRecord<>(PATCH_TOPIC,
        "{\"documentId\":\"road0\",\"operations\":[{\"op\":\"add\",\"path\":\"\",\"value\":{\"name\":\"hello\"}}]}"));

    while (true) {
      JsonNode result = store.get("road0");
      if (result != null) {
        assertThat(result.path("name").textValue(), is("hello"));
        return;
      }
      Thread.sleep(100);
    }
  }

  @Test(timeout = 20000)
  public void bad_patchs_are_skipped() throws Exception {
    store.put("road1", mapper.readTree("{\"name\":\"hello\"}"));

    producer.send(new ProducerRecord<>(PATCH_TOPIC, "not json"));
    producer.send(new ProducerRecord<>(PATCH_TOPIC,
        "{\"documentId\":\"road1\",\"operations\":[{\"op\":\"replace\",\"path\":\"/name\",\"value\":\"hi there\"}]}"));

    while (!"hi there".equals(store.get("road1").path("name").asText())) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 20000)
  public void remove_document() throws Exception {
    store.put("road2", mapper.readTree("{\"name\":\"goodbye\"}"));

    producer.send(new ProducerRecord<>(PATCH_TOPIC,
        "{\"documentId\":\"road2\",\"operations\":[{\"op\":\"remove\",\"path\":\"\"}]}"));

    while (store.containsKey("road2")) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 20000)
  public void testMetrics() throws Exception {
    RestTemplate restTemplate = new RestTemplate();
    String fooResourceUrl = "http://localhost:" + port + "/actuator/prometheus";

    Awaitility.await().atMost(5, SECONDS).pollInterval(100, MILLISECONDS).until(() -> {
      ResponseEntity<String> response = restTemplate.getForEntity(fooResourceUrl, String.class);
      assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
      List<String> lines = Arrays.asList(response.getBody().split("\n")).stream().filter(l -> !l.startsWith("#")).collect(toList());
      assertThat(lines, not(emptyList()));
    });
  }

  @AfterClass
  public static void after() {
    if (context != null) {
      context.close();
    }
  }
}
