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
package com.hotels.road.trafficcontrol.it;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.boot.Banner.Mode.OFF;

import static com.google.common.base.Charsets.UTF_8;

import static com.hotels.road.tollbooth.client.api.Operation.ADD;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.kafkastore.exceptions.SerializationException;
import com.hotels.road.kafkastore.serialization.Serializer;
import com.hotels.road.rest.model.RoadType;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.trafficcontrol.KafkaAdminClient;
import com.hotels.road.trafficcontrol.KafkaModelReader;
import com.hotels.road.trafficcontrol.TrafficControl;
import com.hotels.road.trafficcontrol.TrafficControlApp;
import com.hotels.road.trafficcontrol.model.KafkaRoad;
import com.hotels.road.trafficcontrol.model.TrafficControlStatus;

import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class TrafficControlIntegrationTest {
  static final String TOPIC_CREATED_PATH = "/status/topicCreated";
  static final String KAFKA_STATUS_MESSAGE_PATH = "/status/message";
  static final String KAFKA_STATUS_PATH = "/status";
  static final String TOPIC_PARTITIONS_PATH = "/status/partitions";
  static final String TOPIC_REPLICATION_FACTOR_PATH = "/status/replicationFactor";

  private static final String TEST_ROAD_NAME_PREFIX = "test_road";
  private static final String ROAD_PREFIX = "road.";
  private static final String TOPIC_NAME_PREFIX = ROAD_PREFIX + TEST_ROAD_NAME_PREFIX;
  private static final String ROAD_TOPIC = "road";
  private static final String PATCH_TOPIC = "patch";

  private static final int NUM_BROKERS = 1;

  private final ObjectMapper mapper = new ObjectMapper();

  private static KafkaStore<String, JsonNode> kafkaStore;

  @ClassRule
  public static EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(NUM_BROKERS);

  private static ConfigurableApplicationContext context;

  @BeforeClass
  public static void before() throws Exception {
    kafka.createTopic(ROAD_TOPIC, 1, 1);
    kafka.createTopic(PATCH_TOPIC, 1, 1);

    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    context = new SpringApplicationBuilder(TrafficControlApp.class)
        .bannerMode(OFF)
        .properties(
            ImmutableMap
                .<String, Object> builder()
                .put("server.port", Integer.toString(port))
                .put("kafka.default.replicationFactor", "1")
                .put("kafka.bootstrapServers", kafka.bootstrapServers())
                .put("kafka.zookeeper", kafka.zKConnectString())
                .put("kafka.road.topic", ROAD_TOPIC)
                .put("kafka.road.modification.topic", PATCH_TOPIC)
                .build())
        .build()
        .run();

    kafkaStore = new KafkaStore<>(kafka.bootstrapServers(), new StringJsonNodeSerializer(), ROAD_TOPIC);
  }

  @Test(timeout = 20000)
  public void createTopic() throws Exception {
    ObjectNode model = mapper.createObjectNode();
    model.set("name", TextNode.valueOf(TOPIC_NAME_PREFIX + "_1"));
    model.set("topicName", TextNode.valueOf(TOPIC_NAME_PREFIX + "_1"));
    kafkaStore.put(TEST_ROAD_NAME_PREFIX + "_1", model);

    for (String messageString : fetchMessages(createPatchTopicConsumer(), 1)) {
      log.info(messageString);
      JsonNode message = mapper.readTree(messageString);
      assertThat(message.get("documentId").asText(), is(TEST_ROAD_NAME_PREFIX + "_1"));
      model = (ObjectNode) JsonPatch.fromJson(message.get("operations")).apply(model);
    }

    assertThat(model.get("status").get("topicCreated").asBoolean(), is(true));
    assertThat(model.get("status").get("partitions").asInt(), is(6));
    assertThat(model.get("status").get("replicationFactor").asInt(), is(1));
  }

  @Test(timeout = 20000)
  public void inspect_updates_status_when_required() throws Exception {
    kafka.createTopic("test_topic2", 4, 1);
    JsonNode model = mapper
        .readTree(
            "{\"name\":\"test\",\"topicName\":\"test_topic2\",\"status\":{\"topicCreated\":true,\"partitions\":999,\"replicationFactor\":999}}");
    KafkaModelReader modelReader = context.getBean(KafkaModelReader.class);
    TrafficControl agent = context.getBean(TrafficControl.class);
    List<PatchOperation> operations = agent.inspectModel("test", modelReader.read(model));

    JsonNode message = mapper.convertValue(operations, JsonNode.class);
    model = JsonPatch.fromJson(message).apply(model);

    assertThat(model.get("status").get("topicCreated").asBoolean(), is(true));
    assertThat(model.get("status").get("partitions").asInt(), is(4));
    assertThat(model.get("status").get("replicationFactor").asInt(), is(1));
  }

  @Test(timeout = 20000)
  public void inspect_does_nothing_when_all_is_ok() throws Exception {
    kafka.createTopic("test_topic", 4, 1);
    JsonNode model = mapper
        .readTree(
            "{\n"
                + "  \"name\": \"test\",\n"
                + "  \"topicName\": \"test_topic\",\n"
                + "  \"type\": \"NORMAL\",\n"
                + "  \"status\": {\n"
                + "    \"topicCreated\": true,\n"
                + "    \"partitions\": 4,\n"
                + "    \"replicationFactor\": 1,\n"
                + "    \"message\": \"\"\n"
                + "  }\n"
                + "}");
    KafkaModelReader modelReader = context.getBean(KafkaModelReader.class);
    TrafficControl agent = context.getBean(TrafficControl.class);
    List<PatchOperation> operations = agent.inspectModel("test", modelReader.read(model));

    assertThat(operations, is(empty()));
  }

  @Test(timeout = 20000)
  public void inspect_creates_topic_if_missing() throws Exception {
    KafkaRoad model = new KafkaRoad("test", "test_topic3", RoadType.NORMAL, new TrafficControlStatus(true, 6, 1, ""), null);

    context.getBean(TrafficControl.class).inspectModel("test", model);

    assertTrue(context.getBean(KafkaAdminClient.class).topicExists("test_topic3"));
  }

  @Test
  public void topic_updated_with_throttle_props() throws Exception {
    kafka.createTopic("test_topic4", 4, 1);
    JsonNode model = mapper
        .readTree(
            "{\"topicName\":\"test_topic4\",\"status\":{\"topicCreated\":true,\"partitions\":4,\"replicationFactor\":1}}");
    KafkaModelReader modelReader = context.getBean(KafkaModelReader.class);
    TrafficControl agent = context.getBean(TrafficControl.class);
    agent.inspectModel("test", modelReader.read(model));

    ZkUtils zkUtils = ZkUtils.apply(kafka.zKConnectString(), 60000, 60000, false);
    Properties config = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test_topic4");
    zkUtils.close();

    assertThat(config.getProperty("leader.replication.throttled.replicas"), is("*"));
    assertThat(config.getProperty("follower.replication.throttled.replicas"), is("*"));
  }

  @Test
  public void topic_created_with_throttle_props() throws Exception {
    JsonNode model = mapper
        .readTree(
            "{\"topicName\":\"test_topic5\",\"status\":{\"topicCreated\":true,\"partitions\":4,\"replicationFactor\":1}}");
    KafkaModelReader modelReader = context.getBean(KafkaModelReader.class);
    TrafficControl agent = context.getBean(TrafficControl.class);
    agent.inspectModel("test", modelReader.read(model));

    ZkUtils zkUtils = ZkUtils.apply(kafka.zKConnectString(), 60000, 60000, false);
    Properties config = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test_topic5");
    zkUtils.close();

    assertThat(config.getProperty("leader.replication.throttled.replicas"), is("*"));
    assertThat(config.getProperty("follower.replication.throttled.replicas"), is("*"));
  }

  private <T> List<T> fetchMessages(KafkaConsumer<?, T> consumer, int messagesCount) {
    List<T> result = new ArrayList<>();
    while (result.size() < messagesCount) {
      ConsumerRecords<?, T> consumerRecords = consumer.poll(100L);
      for (ConsumerRecord<?, T> record : consumerRecords) {
        result.add(record.value());
      }
    }
    return result;
  }

  @Test
  public void compact_road_created_correctly() throws Exception {
    KafkaRoad model = new KafkaRoad("test_topic6", "road.test_topic6", RoadType.COMPACT, null, null);

    context.getBean(TrafficControl.class).newModel("test_topic6", model);

    ZkUtils zkUtils = ZkUtils.apply(kafka.zKConnectString(), 60000, 60000, false);
    Properties config = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "road.test_topic6");
    zkUtils.close();

    assertThat(config.getProperty(TopicConfig.CLEANUP_POLICY_CONFIG), is(TopicConfig.CLEANUP_POLICY_COMPACT));
  }

  @Test(timeout = 20000)
  public void inspect_messagestatus() throws Exception {
    KafkaRoad model = new KafkaRoad("test", "test_topic3", RoadType.NORMAL, new TrafficControlStatus(true, 6, 1, ""), null);
    context.getBean(TrafficControl.class).inspectModel("test", model);
    final List<PatchOperation> patchOperations = context.getBean(KafkaAdminClient.class).updateMessageStatus(model);
    assertThat(patchOperations.size(), is(1));
    assertThat(patchOperations.get(0).getOperation(), is(ADD));
    assertThat(patchOperations.get(0).getPath(), is("/messagestatus"));
  }

  private KafkaConsumer<String, String> createPatchTopicConsumer() {
    return createKafkaConsumer(kafka.bootstrapServers(), PATCH_TOPIC, "test_consumer", mapper);
  }

  private KafkaConsumer<String, String> createKafkaConsumer(
      String bootstrapServers,
      String patchTopic,
      String patchGroupId,
      ObjectMapper objectMapper) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", bootstrapServers);
    properties.setProperty("group.id", patchGroupId);
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("enable.auto.commit", "false");
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
        properties,
        new StringDeserializer(),
        new StringDeserializer());
    kafkaConsumer.subscribe(Lists.newArrayList(patchTopic));
    return kafkaConsumer;
  }

  @AfterClass
  public static void after() {
    if (context != null) {
      context.close();
    }
  }

  private static class StringJsonNodeSerializer implements Serializer<String, JsonNode> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serializeKey(String key) throws SerializationException {
      return key.getBytes(UTF_8);
    }

    @Override
    public byte[] serializeValue(JsonNode value) throws SerializationException {
      try {
        return mapper.writeValueAsBytes(value);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String deserializeKey(byte[] key) throws SerializationException {
      return new String(key, UTF_8);
    }

    @Override
    public JsonNode deserializeValue(byte[] value) throws SerializationException {
      try {
        return mapper.readTree(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
