/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import kafka.log.LogConfig;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.kafkastore.serialization.Serializer;
import com.hotels.road.kafkastore.serialization.StringSerializer;

public class KafkaStoreTest {
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
  public void setAndThenGetValue() throws Exception {
    String key = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.put(key, "test-value");
      String value = store.get(key);
      assertThat(value, is("test-value"));
    }
  }

  @Test
  public void setAndThenRemoveValue() throws Exception {
    String key = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.put(key, "test-value");
      String value = store.get(key);
      assertThat(value, is("test-value"));
      store.remove(key);
      assertThat(store.containsKey(key), is(false));
      assertThat(store.get(key), is(nullValue()));
    }
  }

  @Test
  public void setAndGetValueInAnotherStore() throws Exception {
    String key = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store1 = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC);
        KafkaStore<String, String> store2 = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store1.put(key, "test-value");

      store2.sync();
      String value = store2.get(key);
      assertThat(value, is("test-value"));
    }
  }

  @Test
  public void valueStillAvailableAfterOriginalStoreCloses() throws Exception {
    String key = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.put(key, "test-value");
      String value = store.get(key);
      assertThat(value, is("test-value"));
    }

    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      String value = store.get(key);
      assertThat(value, is("test-value"));
    }
  }

  @Test
  public void putAllKeysImmediatelyAvailable() throws Exception {
    String key1 = UUID.randomUUID().toString();
    String key2 = UUID.randomUUID().toString();
    String key3 = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.putAll(ImmutableMap.<String, String> builder().put(key1, "1").put(key2, "2").put(key3, "3").build());
      assertThat(store.get(key1), is("1"));
      assertThat(store.get(key2), is("2"));
      assertThat(store.get(key3), is("3"));
    }
  }

  @Test
  public void verifySizeIsCorrectAfterClear() throws Exception {
    String key1 = UUID.randomUUID().toString();
    String key2 = UUID.randomUUID().toString();
    String key3 = UUID.randomUUID().toString();
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.clear();
      assertThat(store.size(), is(0));
      store.putAll(ImmutableMap.<String, String> builder().put(key1, "1").put(key2, "2").put(key3, "3").build());
      assertThat(store.size(), is(3));
      store.clear();
      assertThat(store.size(), is(0));
      assertThat(store.isEmpty(), is(true));
    }
  }

  @Test
  public void nullKeyIsStored() throws Exception {
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.put(null, "value");
      assertThat(store.get(null), is("value"));
      assertThat(store.containsKey(null), is(true));
    }
  }

  @Test
  public void nullValueIsAccepted() throws Exception {
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.put("key", null);
      assertThat(store.get("key"), is(nullValue()));
      assertThat(store.containsKey("key"), is(true));
    }
  }

  @Test
  public void nullKeysWorkDuringPutAll() throws Exception {
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.clear();
      Map<String, String> map = new HashMap<>();
      map.put("key", "value1");
      map.put(null, "value2");
      store.putAll(map);
      assertThat(store.size(), is(2));
      assertThat(store.get(null), is("value2"));
      assertThat(store.get("key"), is("value1"));
    }
  }

  @Test
  public void nullValueInPutAllWorks() throws Exception {
    try (KafkaStore<String, String> store = new KafkaStore<>(CLUSTER.bootstrapServers(), SERIALIZER, TOPIC)) {
      store.clear();
      Map<String, String> map = new HashMap<>();
      map.put("key1", "value");
      map.put("key2", null);
      store.putAll(map);
      assertThat(store.get("key1"), is("value"));
      assertThat(store.get("key2"), is(nullValue()));
      assertThat(store.containsKey("key2"), is(true));
    }
  }
}
