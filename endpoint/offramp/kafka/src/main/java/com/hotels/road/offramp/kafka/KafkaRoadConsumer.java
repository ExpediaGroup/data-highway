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
package com.hotels.road.offramp.kafka;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;

import static lombok.AccessLevel.PACKAGE;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.Deserializer;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.FluentIterable;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.SchemaProvider;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.offramp.spi.RoadConsumer;

@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class KafkaRoadConsumer implements RoadConsumer {
  static final String GROUP_ID_PREFIX = "offramp";
  private static final Deserializer<Void> keyDeserializer = new NullDeserializer();
  private static final Deserializer<Payload<byte[]>> valueDeserializer = new PayloadDeserializer();

  private final @Getter(PACKAGE) Properties properties;
  private final @Getter(PACKAGE) String topic;
  private final String roadName;
  private final SchemaProvider schemaProvider;
  private final AvroPayloadDecoder payloadDecoder;
  private final long pollTimeoutMillis;
  private final int minMaxPollRecords;
  private final int maxMaxPollRecords;
  private Consumer<Void, Payload<byte[]>> consumer;

  public KafkaRoadConsumer(
      Properties properties,
      String topic,
      String roadName,
      SchemaProvider schemaProvider,
      long pollTimeoutMillis,
      int minMaxPollRecords,
      int maxMaxPollRecords) {
    this(properties, topic, roadName, schemaProvider, new AvroPayloadDecoder(), pollTimeoutMillis, minMaxPollRecords,
        maxMaxPollRecords);
  }

  @Override
  public void init(long initialRequest, RebalanceListener rebalanceListener) {
    long maxPollRecords = min(max(initialRequest, minMaxPollRecords), maxMaxPollRecords);
    properties.setProperty("max.poll.records", Long.toString(maxPollRecords));
    consumer = createConsumer();
    try {
      consumer.subscribe(singletonList(topic), new KafkaRebalanceListener(rebalanceListener));
    } catch (UnknownTopicOrPartitionException e) {
      consumer.close();
      throw new RuntimeException("Unknown topic: " + topic, e);
    }
  }

  @Override
  public Iterable<Record> poll() {
    return FluentIterable.from(consumer.poll(pollTimeoutMillis)).transform(r -> {
      Payload<byte[]> p = r.value();
      Schema schema = schemaProvider.schema(roadName, p.getSchemaVersion());
      JsonNode message = payloadDecoder.decode(schema, p.getMessage());
      Payload<JsonNode> payload = new Payload<>(p.getFormatVersion(), p.getSchemaVersion(), message);
      return new Record(r.partition(), r.offset(), r.timestamp(), payload);
    }).toList();
  }

  @Override
  public boolean commit(Map<Integer, Long> offsets) {
    Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = new HashMap<>(offsets.size());
    offsets.forEach((partition, offset) -> {
      kafkaOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
    });
    try {
      consumer.commitSync(kafkaOffsets);
      return true;
    } catch (CommitFailedException e) {
      throw e;
    } catch (Exception e) {
      log.error("Could not commit offsets for topic '{}': {}", topic, offsets, e);
      return false;
    }
  }

  @Override
  public void close() {
    try {
      consumer.unsubscribe();
      consumer.close();
      log.info("Closed KafkaConsumer");
    } catch (org.apache.kafka.common.errors.InterruptException e) {
      log.error("Interrupted while closing KafkaConsumer");
      Thread.currentThread().interrupt();
    }
  }

  Consumer<Void, Payload<byte[]>> createConsumer() {
    return new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
  }

  @RequiredArgsConstructor
  public static class Factory implements RoadConsumer.Factory {
    private final String bootstrapServers;
    private final Map<String, Road> store;
    private final long pollTimeoutMillis;
    private final int minMaxPollRecords;
    private final int maxMaxPollRecords;
    private final SchemaProvider schemaProvider;

    @Override
    public RoadConsumer create(
        @NonNull String roadName,
        @NonNull String streamName,
        @NonNull DefaultOffset defaultOffset)
      throws UnknownRoadException {
      String topic = Optional.of(roadName).map(store::get).map(Road::getTopicName).orElseThrow(
          () -> new UnknownRoadException("Unknown road: " + roadName));

      Properties properties = new Properties();
      properties.setProperty("bootstrap.servers", bootstrapServers);
      properties.setProperty("group.id", String.format("%s-%s-%s", GROUP_ID_PREFIX, roadName, streamName));
      properties.setProperty("auto.offset.reset", defaultOffset.name().toLowerCase());
      properties.setProperty("enable.auto.commit", "false");

      return new KafkaRoadConsumer(properties, topic, roadName, schemaProvider, pollTimeoutMillis, minMaxPollRecords,
          maxMaxPollRecords);
    }
  }

  @Slf4j
  @RequiredArgsConstructor
  static class KafkaRebalanceListener implements ConsumerRebalanceListener {
    private @NonNull @Getter final RebalanceListener rebalanceListener;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      // no-op for now
      log.debug("Partitions Revoked: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      rebalanceListener.onRebalance(partitions.stream().map(TopicPartition::partition).collect(toSet()));
    }
  }
}
