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
package com.hotels.road.tollbooth.client.kafka;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

public class KafkaPatchSetEmitter implements PatchSetEmitter {

  private final ObjectWriter writer;
  private final Producer<String, String> kafkaProducer;
  private final String trafficControlTopic;

  public KafkaPatchSetEmitter(String trafficControlTopic, Producer<String, String> kafkaProducer) {
    this(trafficControlTopic, kafkaProducer, new ObjectMapper());
  }

  public KafkaPatchSetEmitter(String trafficControlTopic, Producer<String, String> kafkaProducer, ObjectMapper mapper) {
    this.trafficControlTopic = trafficControlTopic;
    this.kafkaProducer = kafkaProducer;
    writer = mapper.writer();
  }

  @Override
  public void emit(PatchSet patchSet) {
    try {
      ProducerRecord<String, String> record = new ProducerRecord<>(trafficControlTopic, patchSet.getDocumentId(),
          writer.writeValueAsString(patchSet));
      kafkaProducer.send(record, this::updateMetrics).get();
    } catch (InterruptedException | ExecutionException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void updateMetrics(RecordMetadata metadata, Exception e) {}

}
