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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.boot.DataHighwayApplication;
import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.kafkastore.KafkaStoreUtils;

@SpringBootApplication
public class TollboothApp {
  private static final int SESSION_TIMEOUT_MS = (int) SECONDS.toMillis(10);
  private static final int CONNECTION_TIMEOUT_MS = (int) SECONDS.toMillis(8);
  private static final boolean IS_SECURE_KAFKA_CLUSTER = false;

  @Bean
  public Map<String, JsonNode> store(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.store.topic}") String topic,
      @Value("${kafka.store.replicas:3}") int replicas,
      @Value("${kafka.zookeeper}") String zkConnect,
      @Value("${tollbooth.model.producer.compression:gzip}") String compressionType,
      @Value("${tollbooth.model.producer.maxRequestSize:1048576}") int maxRequestSize,
      ObjectMapper mapper)
    throws InterruptedException {

    KafkaStoreUtils.checkAndCreateTopic(zkConnect, topic, replicas);
    Thread.sleep(1000L);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(COMPRESSION_TYPE_CONFIG, compressionType);
    producerProps.put(MAX_REQUEST_SIZE_CONFIG, maxRequestSize);

    return new KafkaStore<>(bootstrapServers, new JsonNodeSerializer(mapper), topic, producerProps, emptyMap());
  }

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  @Bean(destroyMethod = "close")
  public Consumer<String, String> patchConsumer(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.patch.topic}") String topic,
      @Value("${kafka.patch.groupId}") String groupId,
      @Value("${kafka.patch.partitions:1}") int partitions,
      @Value("${kafka.patch.replicas:3}") int replicas,
      @Value("${kafka.zookeeper}") String zkConnect)
    throws InterruptedException {

    checkAndCreateTopic(zkConnect, topic, partitions, replicas);
    Thread.sleep(1000L);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", bootstrapServers);
    properties.setProperty("group.id", groupId);
    properties.setProperty("auto.offset.reset", "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
        new StringDeserializer());
    consumer.subscribe(singleton(topic));

    return consumer;
  }

  private void checkAndCreateTopic(String zkConnect, String topic, int partitions, int replicas) {
    ZkClient zkClient = new ZkClient(zkConnect, SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS, ZKStringSerializer$.MODULE$);
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), IS_SECURE_KAFKA_CLUSTER);

    if (!AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, partitions, replicas, new Properties(), RackAwareMode.Enforced$.MODULE$);
    }

    zkUtils.close();
    zkClient.close();
  }

  public static void main(String[] args) {
    DataHighwayApplication.run(TollboothApp.class, args);
  }
}
