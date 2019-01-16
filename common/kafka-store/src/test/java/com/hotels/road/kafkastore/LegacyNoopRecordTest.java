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
package com.hotels.road.kafkastore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import kafka.log.LogConfig;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.kafkastore.serialization.Serializer;
import com.hotels.road.kafkastore.serialization.StringSerializer;

public class LegacyNoopRecordTest {
  private static final Serializer<String, String> SERIALIZER = new StringSerializer();
  private static final String TOPIC = "store-test";
  private static final int NUM_BROKERS = 1;

  @ClassRule
  public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties topicConfig = new Properties();
    topicConfig.setProperty(LogConfig.CleanupPolicyProp(), "compact");
    CLUSTER.createTopic(TOPIC, 1, 1, topicConfig);
  }

  @Test
  public void noopKeyIsIgnored() throws Exception {
    String key = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC);
        Producer<byte[], byte[]> producer = new KafkaProducer<>(
            ImmutableMap
                .<String, Object> builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName())
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName())
                .build())) {
      store.clear();
      store.put(key, "test-value");
      assertThat(store.size(), is(1));
      assertThat(store.get(key), is("test-value"));
      producer.send(new ProducerRecord<byte[], byte[]>(TOPIC, new byte[] { 0x00 }, null)).get();
      store.sync();
      assertThat(store.size(), is(1));
    }
  }
}
