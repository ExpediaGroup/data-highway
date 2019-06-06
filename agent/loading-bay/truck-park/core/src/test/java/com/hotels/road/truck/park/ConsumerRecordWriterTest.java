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
package com.hotels.road.truck.park;

import static org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.io.AbortableOutputStream;
import com.hotels.road.truck.park.metrics.Metrics;
import com.hotels.road.truck.park.spi.AbortableOutputStreamFactory;
import com.hotels.road.truck.park.spi.RecordWriter;
import com.hotels.road.truck.park.spi.Writer;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ConsumerRecordWriterTest {

  private static final String LOCATION = "location";

  private final Map<Schema, Writer<Record>> writers = new HashMap<>();

  private @Mock Supplier<String> locationSupplier;
  private @Mock RecordWriter.Factory recordWriterFactory;
  private @Mock Writer<Record> recordWriter;
  private @Mock AbortableOutputStreamFactory outputStreamFactory;
  private @Mock AbortableOutputStream abortableOutputStream;

  @Mock
  private Metrics metrics;

  private final long fileSize = 2;

  private final Schema schema1 = SchemaBuilder
      .record("r")
      .fields()
      .name("foo")
      .type()
      .stringType()
      .noDefault()
      .endRecord();

  private ConsumerRecordWriter underTest;

  private ConsumerRecord<Void, Record> record(Schema schema, Object value, long offset, int size) {
    Record record = new Record(schema);
    record.put("foo", value);
    return new ConsumerRecord<>("topic", 0, offset, 2, LOG_APPEND_TIME, 0, 0, size, null, record);
  }

  @Before
  public void before() throws IOException {
    when(locationSupplier.get()).thenReturn(LOCATION);

    underTest = new ConsumerRecordWriter(writers, locationSupplier, recordWriterFactory, outputStreamFactory, fileSize,
        metrics);
  }

  @Test
  public void write_NoFlush() throws IOException {
    when(outputStreamFactory.create(LOCATION)).thenReturn(abortableOutputStream);
    ArgumentCaptor<OutputStream> captor = ArgumentCaptor.forClass(OutputStream.class);
    when(recordWriterFactory.create(eq(schema1), captor.capture())).thenReturn(recordWriter);

    ConsumerRecord<Void, Record> record = record(schema1, "foo", 1, 10);
    underTest.write(record);

    verify(recordWriter).write(record.value());
    assertThat(underTest.getRecordCounter().get(), is(1L));
    verify(metrics).consumedBytes(10);
    verify(metrics).offsetHighwaterMark(0, 1);
    assertThat(writers.size(), is(1));
  }

  @Test
  public void write_Flush() throws IOException {
    when(outputStreamFactory.create(LOCATION)).thenReturn(abortableOutputStream);
    ArgumentCaptor<OutputStream> captor = ArgumentCaptor.forClass(OutputStream.class);
    when(recordWriterFactory.create(eq(schema1), captor.capture())).thenReturn(recordWriter);

    underTest.getByteCounter().getAndAdd(3L); // fake some written bytes
    ConsumerRecord<Void, Record> record = record(schema1, "foo", 1, 10);
    underTest.write(record);

    verify(recordWriter).write(record.value());
    assertThat(underTest.getRecordCounter().get(), is(0L));
    verify(metrics).consumedBytes(10);
    verify(metrics).offsetHighwaterMark(0, 1);
    verify(metrics).uploadedBytes(3L);
    verify(metrics).uploadedEvents(1L);
    assertThat(writers.size(), is(0));
  }

  @Test
  public void write_Close() throws IOException {
    when(outputStreamFactory.create(LOCATION)).thenReturn(abortableOutputStream);
    ArgumentCaptor<OutputStream> captor = ArgumentCaptor.forClass(OutputStream.class);
    when(recordWriterFactory.create(eq(schema1), captor.capture())).thenReturn(recordWriter);

    underTest.getByteCounter().getAndAdd(1L); // fake some written bytes
    ConsumerRecord<Void, Record> record = record(schema1, "foo", 1, 10);
    underTest.write(record);
    underTest.close();

    verify(recordWriter).write(record.value());
    assertThat(underTest.getRecordCounter().get(), is(0L));
    verify(metrics).consumedBytes(10);
    verify(metrics).offsetHighwaterMark(0, 1);
    verify(metrics).uploadedBytes(1L);
    verify(metrics).uploadedEvents(1L);
    assertThat(writers.size(), is(0));
  }

  @Test
  public void newRecordWriter_ByteCounter() throws IOException {
    when(outputStreamFactory.create(LOCATION)).thenReturn(abortableOutputStream);
    ArgumentCaptor<OutputStream> captor = ArgumentCaptor.forClass(OutputStream.class);
    when(recordWriterFactory.create(eq(schema1), captor.capture())).thenReturn(recordWriter);

    underTest.newRecordWriter(schema1);

    captor.getValue().write(0);
    assertThat(underTest.getByteCounter().get(), is(1L));
  }

  @Test(expected = RuntimeException.class)
  public void newRecordWriter_newOutputStreamException() throws IOException {
    doThrow(IOException.class).when(outputStreamFactory).create(LOCATION);

    underTest.newRecordWriter(schema1);
  }

  @Test(expected = RuntimeException.class)
  public void newRecordWriter_newRecordWriterException() throws IOException {
    when(outputStreamFactory.create(LOCATION)).thenReturn(abortableOutputStream);
    doThrow(IOException.class).when(recordWriterFactory).create(eq(schema1), any(OutputStream.class));

    underTest.newRecordWriter(schema1);
  }
}
