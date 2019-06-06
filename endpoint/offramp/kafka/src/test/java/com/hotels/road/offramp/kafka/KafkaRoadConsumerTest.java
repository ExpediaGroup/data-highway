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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.road.offramp.model.DefaultOffset.EARLIEST;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import avro.shaded.com.google.common.collect.Iterables;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.SchemaProvider;
import com.hotels.road.offramp.kafka.KafkaRoadConsumer.KafkaRebalanceListener;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.offramp.spi.RoadConsumer.RebalanceListener;

@RunWith(MockitoJUnitRunner.Silent.class)
public class KafkaRoadConsumerTest {
  private @Mock Consumer<Void, Payload<byte[]>> consumer;
  private @Mock SchemaProvider schemaProvider;
  private @Mock AvroPayloadDecoder payloadDecoder;
  private @Mock RebalanceListener rebalanceListener;

  private final ObjectMapper mapper = new ObjectMapper();
  private final Schema schema = SchemaBuilder.record("r").fields().optionalString("f").endRecord();

  private final String topicName = "topicName";
  private final String bootstrapServers = "bootstrapServers";
  private final String roadName = "roadName";
  private final String streamName = "streamName";
  private final DefaultOffset defaultOffset = EARLIEST;
  private final Map<Integer, SchemaVersion> schemas = singletonMap(1, new SchemaVersion(schema, 1, false));
  private final Road model = new Road();
  private final Map<String, Road> store = singletonMap(roadName, model);
  private final TopicPartition topicPartition = new TopicPartition(topicName, 0);
  private final long pollTimeout = 100;
  private final Properties properties = new Properties();

  private KafkaRoadConsumer underTest;

  @Before
  public void before() {
    model.setTopicName(topicName);
    model.setSchemas(schemas);
    underTest = spy(
        new KafkaRoadConsumer(properties, topicName, roadName, schemaProvider, payloadDecoder, pollTimeout, 5, 10));
    doReturn(consumer).when(underTest).createConsumer();
  }

  @Test
  public void poll() throws Exception {
    Payload<byte[]> payload = new Payload<>((byte) 0, 1, "{}".getBytes(UTF_8));
    ConsumerRecord<Void, Payload<byte[]>> consumerRecord = new ConsumerRecord<>(topicName, 0, 1L, 2L,
        TimestampType.CREATE_TIME, ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE,
        null, payload);
    Map<TopicPartition, List<ConsumerRecord<Void, Payload<byte[]>>>> recordsMaps = singletonMap(topicPartition,
        singletonList(consumerRecord));
    ConsumerRecords<Void, Payload<byte[]>> records = new ConsumerRecords<>(recordsMaps);
    when(consumer.poll(100)).thenReturn(records);
    when(payloadDecoder.decode(any(), any())).thenReturn(mapper.createObjectNode());

    Record record = new Record(0, 1L, 2L, new Payload<JsonNode>((byte) 0, 1, mapper.createObjectNode()));

    underTest.init(1L, rebalanceListener);
    Iterable<Record> result = underTest.poll();

    assertThat(Iterables.size(result), is(1));
    assertThat(Iterables.get(result, 0), is(record));
  }

  @Test
  public void commitSuccess() throws Exception {
    underTest.init(1L, rebalanceListener);
    boolean result = underTest.commit(singletonMap(0, 1L));
    assertThat(result, is(true));
    verify(consumer).commitSync(singletonMap(topicPartition, new OffsetAndMetadata(1L)));
  }

  @Test
  public void commitFailure() throws Exception {
    doThrow(KafkaException.class).when(consumer).commitSync(any());
    underTest.init(1L, rebalanceListener);
    boolean result = underTest.commit(singletonMap(0, 1L));
    assertThat(result, is(false));
    verify(consumer).commitSync(singletonMap(topicPartition, new OffsetAndMetadata(1L)));
  }

  @Test
  public void close() throws Exception {
    underTest.init(1L, rebalanceListener);
    underTest.close();

    verify(consumer).close();
  }

  @Test
  public void factoryPlumbing() throws Exception {
    Mockito.withSettings().verboseLogging();
    KafkaRoadConsumer.Factory underTestFactory = new KafkaRoadConsumer.Factory(bootstrapServers, store, 0L, 100, 1000,
        null);

    KafkaRoadConsumer consumer = (KafkaRoadConsumer) underTestFactory.create(roadName, streamName, defaultOffset);

    Properties properties = consumer.getProperties();
    assertThat(properties.size(), is(4));
    assertThat(properties.getProperty("bootstrap.servers"), is(bootstrapServers));
    assertThat(properties.getProperty("group.id"), is("offramp-roadName-streamName"));
    assertThat(properties.getProperty("enable.auto.commit"), is("false"));
    assertThat(properties.getProperty("auto.offset.reset"), is("earliest"));
  }

  @Test
  public void init() throws Exception {
    underTest.init(1L, rebalanceListener);
    ArgumentCaptor<KafkaRebalanceListener> captor = ArgumentCaptor.forClass(KafkaRebalanceListener.class);
    verify(consumer).subscribe(eq(singletonList(topicName)), captor.capture());
    assertThat(captor.getValue().getRebalanceListener(), is(rebalanceListener));
  }

  @Test
  public void initMaxPollRecords_Mid() throws Exception {
    underTest.init(7L, rebalanceListener);
    assertThat(underTest.getProperties().get("max.poll.records"), is("7"));
  }

  @Test
  public void initMaxPollRecords_Min() throws Exception {
    underTest.init(4L, rebalanceListener);
    assertThat(underTest.getProperties().get("max.poll.records"), is("5"));
  }

  @Test
  public void initMaxPollRecords_Max() throws Exception {
    underTest.init(11L, rebalanceListener);
    assertThat(underTest.getProperties().get("max.poll.records"), is("10"));
  }
}
