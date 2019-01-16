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

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ConfigurableApplicationContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TruckParkTest {

  @Mock
  private KafkaConsumer<Void, Record> consumer;
  @Mock
  private ConsumerRecordWriter writer;
  @Mock
  private ConfigurableApplicationContext context;

  private final String topic = "topic";
  private final TopicPartition partition = new TopicPartition(topic, 0);
  private final Set<TopicPartition> partitions = Collections.singleton(partition);
  private final Map<TopicPartition, Offsets> offsets = ImmutableMap.of(partition, new Offsets(0, 1L, 2L));
  private final long pollTimeout = 100;

  private TruckPark underTest;

  @Before
  public void before() {
    underTest = new TruckPark(consumer, writer, offsets, pollTimeout, context);
  }

  @Test
  public void test() throws Exception {
    Schema schema = SchemaBuilder.record("r").fields().name("f").type().stringType().noDefault().endRecord();
    Record value = new Record(schema);

    ConsumerRecord<Void, Record> record1 = new ConsumerRecord<>(topic, 0, 1, null, value);
    ConsumerRecords<Void, Record> records1 = new ConsumerRecords<>(
        ImmutableMap.of(partition, ImmutableList.of(record1)));

    when(consumer.poll(pollTimeout)).thenReturn(records1);

    underTest.run(null);

    InOrder inOrder = inOrder(consumer, writer, context);
    inOrder.verify(consumer).assign(partitions);
    inOrder.verify(consumer).seek(partition, 1L);
    inOrder.verify(consumer).poll(pollTimeout);
    inOrder.verify(writer).write(record1);
    inOrder.verify(consumer).pause(partitions);
    inOrder.verify(writer).close();
    inOrder.verify(context).close();
  }

}
