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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ForwardingMap;

import com.hotels.road.kafkastore.exceptions.IgnoreableNoopKeyException;
import com.hotels.road.kafkastore.serialization.Serializer;

public class KafkaStore<K, V> extends ForwardingMap<K, V> implements Map<K, V>, AutoCloseable {
  private static final String NULL_VALUE_MESSAGE = "Map does not support storing null values";
  private static final String NULL_KEY_MESSAGE = "Map does not support storing null keys";

  private static final Logger log = LoggerFactory.getLogger(KafkaStore.class);

  private final Serializer<K, V> serializer;
  private final String topic;
  private final List<StoreUpdateObserver<K, V>> observers;

  private final KafkaBasedLog<byte[], byte[]> kafkaLog;
  private final Map<K, V> localStore;

  public KafkaStore(String bootstrapServers, Serializer<K, V> serializer, String topic) {
    this(bootstrapServers, serializer, topic, emptyList(), new SystemTime(), emptyMap(), emptyMap());
  }

  public KafkaStore(
      String bootstrapServers,
      Serializer<K, V> serializer,
      String topic,
      Collection<StoreUpdateObserver<K, V>> observers) {
    this(bootstrapServers, serializer, topic, observers, new SystemTime(), emptyMap(), emptyMap());
  }

  public KafkaStore(
      String bootstrapServers,
      Serializer<K, V> serializer,
      String topic,
      Time time,
      Map<String, Object> additionalProducerProps,
      Map<String, Object> additionalConsumerProps) {
    this(bootstrapServers, serializer, topic, emptyList(), time, additionalProducerProps, additionalConsumerProps);
  }

  public KafkaStore(
      String bootstrapServers,
      Serializer<K, V> serializer,
      String topic,
      Map<String, Object> additionalProducerProps,
      Map<String, Object> additionalConsumerProps) {
    this(bootstrapServers, serializer, topic, emptyList(), new SystemTime(), additionalProducerProps,
        additionalConsumerProps);
  }

  public KafkaStore(
      String bootstrapServers,
      Serializer<K, V> serializer,
      String topic,
      Collection<StoreUpdateObserver<K, V>> observers,
      Time time,
      Map<String, Object> additionalProducerProps,
      Map<String, Object> additionalConsumerProps) {
    this.serializer = serializer;
    this.topic = topic;
    this.observers = new ArrayList<>(observers);

    localStore = new HashMap<>();

    kafkaLog = createKafkaLog(bootstrapServers, topic, time, additionalProducerProps, additionalConsumerProps);

    kafkaLog.start();
  }

  @Override
  public V put(K key, V value) {
    try {
      putInternal(key, value);
      sync();
      return value;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    try {
      m.forEach(this::putInternal);
      sync();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void putInternal(K key, V value) {
    byte[] keyBytes = requireNonNull(serializer.serializeKey(key), NULL_KEY_MESSAGE);
    byte[] valueBytes = requireNonNull(serializer.serializeValue(value), NULL_VALUE_MESSAGE);
    sendInternal(keyBytes, valueBytes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object key) {
    try {
      V value = get(key);
      removeInternal((K) key);
      sync();
      return value;
    } catch (ClassCastException e) {
      return null;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    try {
      new ArrayList<>(keySet()).forEach(this::removeInternal);
      sync();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void removeInternal(K key) {
    byte[] keyBytes = requireNonNull(serializer.serializeKey(key), NULL_KEY_MESSAGE);
    sendInternal(keyBytes, null);
  }

  @Override
  public void close() throws Exception {
    kafkaLog.stop();
  }

  @Override
  protected Map<K, V> delegate() {
    return localStore;
  }

  @VisibleForTesting
  void sync() throws InterruptedException {
    try {
      kafkaLog.readToEnd().get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private KafkaBasedLog<byte[], byte[]> createKafkaLog(
      String bootstrapServers,
      String topic,
      Time time,
      Map<String, Object> additionalProducerProps,
      Map<String, Object> additionalConsumerProps) {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.putAll(additionalProducerProps);
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);

    Map<String, Object> consumerProps = new HashMap<>();
    producerProps.putAll(additionalConsumerProps);
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    return new KafkaBasedLog<>(topic, producerProps, consumerProps, this::consume, time, null);
  }

  private void consume(Throwable error, ConsumerRecord<byte[], byte[]> result) {
    byte[] valueBytes = result.value();

    if (result.key() == null) {
      log.warn("Discarding record with invalid null key for topic {}", topic);
      return;
    }

    try {
      K key = serializer.deserializeKey(result.key());
      if (valueBytes == null) {
        V oldValue = localStore.remove(key);
        observers.forEach(observer -> observer.handleRemove(key, oldValue));
      } else {
        V value = serializer.deserializeValue(valueBytes);
        V previous = localStore.put(key, value);
        if (previous == null) {
          observers.forEach(observer -> observer.handleNew(key, value));
        } else {
          observers.forEach(observer -> observer.handleUpdate(key, previous, value));
        }
      }
    } catch (IgnoreableNoopKeyException e) {
      // This is an old NOOP key and should be ignored
    }
  }

  private void sendInternal(byte[] keyBytes, byte[] valueBytes) {
    try {
      CompletableFuture<Void> future = new CompletableFuture<>();
      kafkaLog.send(keyBytes, valueBytes, (metadata, sendException) -> {
        if (sendException == null) {
          future.complete(null);
        } else {
          future.completeExceptionally(sendException);
        }
      });
      future.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }
}
