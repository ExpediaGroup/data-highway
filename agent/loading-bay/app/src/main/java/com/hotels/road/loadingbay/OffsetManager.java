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
package com.hotels.road.loadingbay;

import static java.util.Collections.singleton;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

@Component
public class OffsetManager implements AutoCloseable {
  private static final String GROUP_ID = "hive-agent";

  private final KafkaConsumer<String, String> consumer;

  @Autowired
  public OffsetManager(@Value("${kafka.bootstrapServers}") String brokerList) {
    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", GROUP_ID);
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    consumer = new KafkaConsumer<>(props);
  }

  public Map<Integer, Long> getLatestOffsets(String topicName) {
    synchronized (consumer) {
      ImmutableMap.Builder<Integer, Long> builder = ImmutableMap.builder();
      consumer.endOffsets(topicPartitions(topicName)).forEach((k, v) -> builder.put(k.partition(), v));
      return builder.build();
    }
  }

  public Map<Integer, Long> getCommittedOffsets(String topicName) {
    synchronized (consumer) {
      List<TopicPartition> topicPartitions = topicPartitions(topicName);
      ImmutableMap.Builder<Integer, Long> builder = ImmutableMap.builder();
      topicPartitions.forEach(tp -> {
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        Long offset;
        if (offsetAndMetadata == null) {
          offset = consumer.beginningOffsets(singleton(tp)).get(tp);
        } else {
          offset = offsetAndMetadata.offset();
        }
        builder.put(tp.partition(), offset);
      });
      return builder.build();
    }
  }

  public void commitOffsets(String topicName, Map<Integer, Long> offsets) {
    synchronized (consumer) {
      Map<TopicPartition, OffsetAndMetadata> o = new HashMap<>();
      offsets.forEach((pid, offset) -> o.put(new TopicPartition(topicName, pid), new OffsetAndMetadata(offset)));
      consumer.commitSync(o);
    }
  }

  @Override
  public void close() throws Exception {
    consumer.close();
  }

  private List<TopicPartition> topicPartitions(String topicName) {
    synchronized (consumer) {
      List<TopicPartition> topicPartitions = FluentIterable
          .from(consumer.partitionsFor(topicName))
          .transform(PartitionInfo::partition)
          .transform(pid -> new TopicPartition(topicName, pid))
          .toList();
      return topicPartitions;
    }
  }
}
