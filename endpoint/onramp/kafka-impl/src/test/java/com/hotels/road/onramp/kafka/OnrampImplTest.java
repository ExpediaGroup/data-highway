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
package com.hotels.road.onramp.kafka;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.InvalidKeyException;
import com.hotels.road.exception.RoadUnavailableException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.onramp.api.Event;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;

@RunWith(MockitoJUnitRunner.class)
public class OnrampImplTest {

  private static final String ROAD_NAME = "test-onramp";
  private static final Schema SCHEMA = SchemaBuilder
      .builder()
      .record("r")
      .fields()
      .name("f")
      .type()
      .stringType()
      .noDefault()
      .endRecord();

  @Mock
  private Road road;
  @Mock
  private OnrampMetrics metrics;
  @Mock
  private Producer<byte[], byte[]> kafkaProducer;
  @Mock
  private Future<RecordMetadata> future;

  private final ObjectMapper mapper = new ObjectMapper();
  private OnrampImpl underTest;
  private Map<Integer, SchemaVersion> schemas;

  @Before
  public void setUp() {
    schemas = ImmutableMap.of(1, new SchemaVersion(SCHEMA, 1, false));
    when(road.getName()).thenReturn(ROAD_NAME);
    when(road.getTopicName()).thenReturn(ROAD_NAME);
    when(road.getSchemas()).thenReturn(schemas);
    underTest = new OnrampImpl(metrics, kafkaProducer, road);
  }

  @Test
  public void failsToVerifyEvent()
    throws InvalidEventException, InterruptedException, JsonProcessingException, IOException {
    Future<Boolean> result = underTest.sendEvent(mapper.readTree("{}"));

    try {
      result.get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(InvalidEventException.class));
      return;
    }
    fail("Expected ExecutionException");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendFails()
    throws InvalidEventException, InterruptedException, ExecutionException, JsonProcessingException, IOException {
    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(future);
    doThrow(new ExecutionException(new BufferExhaustedException("exhausted"))).when(future).get();

    Future<Boolean> result = underTest.sendEvent(mapper.readTree("{\"f\": \"f16\"}"));

    try {
      result.get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(BufferExhaustedException.class));
      return;
    }
    fail("Expected ExecutionException");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendSucceeds()
    throws InvalidEventException, InterruptedException, ExecutionException, JsonProcessingException, IOException {
    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(future);

    Future<Boolean> result = underTest.sendEvent(mapper.readTree("{\"f\": \"f16\"}"));

    assertThat(result.get(), is(true));
  }

  @Test
  public void getSchemaWithId() {
    SchemaVersion schemaVersion = underTest.getSchemaVersion();

    assertThat(schemaVersion.getSchema(), is(SCHEMA));
    assertThat(schemaVersion.getVersion(), is(1));
  }

  @Test(expected = RoadUnavailableException.class)
  public void getSchemaWithIdNoSchema() {
    when(road.getSchemas()).thenReturn(Collections.emptyMap());
    underTest.getSchemaVersion();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendEncodedEvent_UpdateMetrics_Success() throws InvalidKeyException {
    RecordMetadata metadata = new RecordMetadata(null, 0, 0, 0, Long.valueOf(0), 0, 1);
    Exception exception = null;

    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      ((Callback) invocation.getArgument(1)).onCompletion(metadata, exception);
      return future;
    });

    underTest.sendEncodedEvent(new Event<>(null, null), null);

    verify(metrics).markSuccessMetrics(ROAD_NAME, 1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendEncodedEvent_UpdateMetrics_Failure() throws InvalidKeyException {
    RecordMetadata metadata = null;
    Exception exception = new Exception();

    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      ((Callback) invocation.getArgument(1)).onCompletion(metadata, exception);
      return future;
    });

    underTest.sendEncodedEvent(new Event<>(null, null), null);

    verify(metrics).markFailureMetrics(ROAD_NAME);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void sendMultiple() throws Exception {
    ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
    when(kafkaProducer.send(captor.capture(), any(Callback.class))).thenReturn(future);

    underTest.sendEvent(mapper.readTree("{\"f\": \"f16\"}"));
    underTest.sendEvent(mapper.readTree("{\"f\": \"f17\"}"));

    List<ProducerRecord> values = captor.getAllValues();

    assertRecord(((ProducerRecord<byte[], byte[]>) values.get(0)).value(), "f16");
    assertRecord(((ProducerRecord<byte[], byte[]>) values.get(1)).value(), "f17");
  }

  private void assertRecord(byte[] value, String expected) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(value);

    assertThat(buffer.get(), is((byte) 0));
    assertThat(buffer.getInt(), is(1));

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, buffer.position(), buffer.remaining(), null);
    Record read = new GenericDatumReader<Record>(SCHEMA).read(null, decoder);
    assertThat(read.get("f").toString(), is(expected));
  }

  @Test
  public void pathSupplier() {
    when(road.getPartitionPath()).thenReturn("$.a");
    Supplier<Path> supplier = OnrampImpl.pathSupplier(road);
    assertThat(supplier.get(), is(KeyPathParser.parse("$.a")));
  }

  @Test
  public void pathSupplierNull() {
    when(road.getPartitionPath()).thenReturn(null);
    Supplier<Path> supplier = OnrampImpl.pathSupplier(road);
    assertThat(supplier.get(), is(nullValue()));
  }

  @Test
  public void pathSupplierEmpty() {
    when(road.getPartitionPath()).thenReturn("");
    Supplier<Path> supplier = OnrampImpl.pathSupplier(road);
    assertThat(supplier.get(), is(nullValue()));
  }

}
