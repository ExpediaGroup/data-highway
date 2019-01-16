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

import static java.util.Collections.singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
class TruckPark implements ApplicationRunner {
  private final KafkaConsumer<Void, Record> consumer;
  private final ConsumerRecordWriter writer;
  private final Map<TopicPartition, Offsets> offsets;
  private final long pollTimeout;
  private final ConfigurableApplicationContext context;

  TruckPark(
      KafkaConsumer<Void, Record> consumer,
      ConsumerRecordWriter writer,
      Map<TopicPartition, Offsets> offsets,
      @Value("${kafka.pollTimeout:100}") long pollTimeout,
      ConfigurableApplicationContext context) {
    this.consumer = consumer;
    this.writer = writer;
    this.offsets = offsets;
    this.pollTimeout = pollTimeout;
    this.context = context;
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    Map<Integer, TopicPartition> running = new HashMap<>();
    offsets.forEach((tp, offset) -> {
      if (offset.getEnd() > offset.getStart()) {
        running.put(tp.partition(), tp);
      }
    });

    consumer.assign(new HashSet<>(running.values()));
    running.values().forEach(partition -> consumer.seek(partition, offsets.get(partition).getStart()));
    log.info("Offsets {}", offsets);
    while (running.size() > 0) {
      log.info("Running {}", running.values());
      for (ConsumerRecord<Void, Record> record : consumer.poll(pollTimeout)) {
        TopicPartition topicPartition = running.get(record.partition());
        if (topicPartition != null) {
          long endOffset = offsets.get(topicPartition).getEnd();
          if (record.offset() < endOffset) {
            writer.write(record);
          }
          if (record.offset() >= endOffset - 1) {
            log.info("Pausing partition {}. Reached offset {}.", record.partition(), record.offset());
            consumer.pause(singleton(topicPartition));
            running.remove(topicPartition.partition());
          }
        }
      }
    }

    log.info("Closing writer");
    writer.close();
    context.close();
  }
}
