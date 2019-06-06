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
package com.hotels.road.tollbooth.client.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.hotels.road.tollbooth.client.api.PatchSet;

@RunWith(MockitoJUnitRunner.class)
public class KafkaPatchSetEmitterTest {

  @Mock
  private Producer<String, String> kafkaProducer;
  private KafkaPatchSetEmitter underTest;
  @Mock
  private Future<RecordMetadata> resultFuture;

  @Mock
  private ObjectMapper mapper;
  @Mock
  private ObjectWriter writer;

  @Before
  public void setUp() {
    underTest = new KafkaPatchSetEmitter("kafka-agent-topic", kafkaProducer);
  }

  @Test
  public void sendPatchSet() {
    when(kafkaProducer.send(ArgumentMatchers.<ProducerRecord<String, String>> any(), ArgumentMatchers.any()))
        .thenReturn(resultFuture);
    underTest.emit(new PatchSet("road1", new ArrayList<>()));
  }

  @Test(expected = RuntimeException.class)
  public void jsonSerializationProblem() throws Exception {
    when(mapper.writer()).thenReturn(writer);
    when(writer.writeValueAsString(any())).thenThrow(JsonProcessingException.class);
    underTest = new KafkaPatchSetEmitter("kafka-agent-topic", kafkaProducer, mapper);
    underTest.emit(new PatchSet("road1", new ArrayList<>()));
  }
}
