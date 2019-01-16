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
package com.hotels.road.truck.park;

import static java.lang.Integer.parseInt;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.time.Clock;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

@Configuration
class TruckParkConfiguration {
  static final String GROUP_ID_PREFIX = "truck-park-";

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  KafkaConsumer<?, Record> consumer(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${road.topic}") String topic,
      Deserializer<Record> deserializer) {
    Map<String, Object> config = ImmutableMap
        .<String, Object> builder()
        .put("bootstrap.servers", bootstrapServers)
        .put("group.id", GROUP_ID_PREFIX + topic)
        .put("enable.auto.commit", "false")
        .put("auto.offset.reset", "earliest")
        .build();
    return new KafkaConsumer<>(config, new NullDeserializer(), deserializer);
  }

  @Bean
  Map<TopicPartition, Offsets> endOffsets(
      @Value("${road.topic}") String topic,
      @Value("${road.offsets}") String offsets) {
    return Splitter.on(';').withKeyValueSeparator(':').split(offsets).entrySet().stream().map(e -> {
      int partition = parseInt(e.getKey());
      List<Long> o = Splitter.on(',').splitToList(e.getValue()).stream().map(Long::parseLong).collect(toList());
      return new Offsets(partition, o.get(0), o.get(1));
    }).collect(toMap(o -> new TopicPartition(topic, o.getPartition()), identity()));
  }
}
