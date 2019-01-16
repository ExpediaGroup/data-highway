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

import static java.util.Collections.singletonList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import kafka.log.LogConfig;

import com.hotels.road.kafkastore.serialization.Serializer;
import com.hotels.road.kafkastore.serialization.StringSerializer;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStoreObserverTest {
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
  public void verifyNewUpdateAndDelete() throws Exception {
    @SuppressWarnings("unchecked")
    StoreUpdateObserver<String, String> observer = mock(StoreUpdateObserver.class);
    String key = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(
        CLUSTER.bootstrapServers(),
        SERIALIZER,
        TOPIC,
        singletonList(observer))) {
      store.put(key, "test-value");
      store.put(key, "test-value-two");
      store.remove(key);

      verify(observer).handleNew(key, "test-value");
      verify(observer).handleUpdate(key, "test-value", "test-value-two");
      verify(observer).handleRemove(key, "test-value-two");
    }
  }

  @Test
  public void verifySecondStoreObservesActionOnFirst() throws Exception {
    @SuppressWarnings("unchecked")
    StoreUpdateObserver<String, String> observer = mock(StoreUpdateObserver.class);
    String key = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC);
        KafkaStore<String, String> store2 = new KafkaStore<>(
            CLUSTER.bootstrapServers(),
            SERIALIZER,
            TOPIC,
            singletonList(observer))) {
      store.put(key, "test-value");
      store.put(key, "test-value-two");
      store.remove(key);

      verify(observer, timeout(10_000)).handleNew(key, "test-value");
      verify(observer, timeout(10_000)).handleUpdate(key, "test-value", "test-value-two");
      verify(observer, timeout(10_000)).handleRemove(key, "test-value-two");
    }
  }
}
