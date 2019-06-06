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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.road.io.AbortableOutputStream;
import com.hotels.road.truck.park.metrics.Metrics;
import com.hotels.road.truck.park.spi.AbortableOutputStreamFactory;
import com.hotels.road.truck.park.spi.RecordWriter;
import com.hotels.road.truck.park.spi.Writer;

@Component
@lombok.RequiredArgsConstructor
class ConsumerRecordWriter implements Writer<ConsumerRecord<Void, Record>> {

  private static final long ZERO = 0L;
  private final Map<Schema, Writer<Record>> writers;
  private final Supplier<String> keySupplier;
  private final RecordWriter.Factory recordWriterFactory;
  private final AbortableOutputStreamFactory outputStreamFactory;
  private final long flushBytesThreshold;
  private final Metrics metrics;
  @lombok.Getter
  private final AtomicLong byteCounter = new AtomicLong(ZERO);
  @lombok.Getter
  private final AtomicLong recordCounter = new AtomicLong(ZERO);

  @Autowired
  ConsumerRecordWriter(
      Supplier<String> keySupplier,
      RecordWriter.Factory recordWriterFactory,
      AbortableOutputStreamFactory outputStreamFactory,
      @Value("${writer.flushBytesThreshold:134217728}") long flushBytesThreshold,
      Metrics metrics) {
    this(new HashMap<>(), keySupplier, recordWriterFactory, outputStreamFactory, flushBytesThreshold, metrics);
  }

  @Override
  public void write(ConsumerRecord<Void, Record> record) throws IOException {
    Schema schema = record.value().getSchema();
    writers.computeIfAbsent(schema, this::newRecordWriter).write(record.value());
    recordCounter.getAndIncrement();
    metrics.consumedBytes(record.serializedValueSize());
    metrics.offsetHighwaterMark(record.partition(), record.offset());
    if (byteCounter.getAndAdd(ZERO) >= flushBytesThreshold) {
      flush();
    }
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  @Override
  public void flush() throws IOException {
    for (Writer<Record> writer : writers.values()) {
      writer.close();
    }
    writers.clear();
    metrics.uploadedBytes(byteCounter.getAndSet(ZERO));
    metrics.uploadedEvents(recordCounter.getAndSet(ZERO));
  }

  @VisibleForTesting
  Writer<Record> newRecordWriter(Schema schema) {
    String key = keySupplier.get();
    AbortableOutputStream outputStream;
    try {
      outputStream = outputStreamFactory.create(key);
    } catch (IOException e) {
      throw new RuntimeException("Error creating output.", e);
    }
    LongConsumer consumer = x -> byteCounter.getAndAdd(x);
    ConsumerCountOutputStream output = new ConsumerCountOutputStream(outputStream, consumer);
    try {
      return recordWriterFactory.create(schema, output);
    } catch (IOException e) {
      throw new RuntimeException("Error creating writer.", e);
    }
  }

}
