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

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;

import com.hotels.jasvorno.JasvornoConverter;
import com.hotels.jasvorno.JasvornoConverterException;
import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.InvalidKeyException;
import com.hotels.road.exception.RoadUnavailableException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.onramp.api.Event;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampTemplate;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;

public class OnrampImpl extends OnrampTemplate<byte[], byte[]> implements Onramp {
  private final OnrampMetrics metrics;
  private final Producer<byte[], byte[]> kafkaProducer;
  private final JsonKeyEncoder keyEncoder;
  private final AvroValueEncoder valueEncoder;

  public OnrampImpl(OnrampMetrics metrics, Producer<byte[], byte[]> kafkaProducer, Road road) {
    super(road);
    this.metrics = metrics;
    this.kafkaProducer = kafkaProducer;
    SchemaVersion schemaVersion = getSchemaVersion();
    keyEncoder = new JsonKeyEncoder(pathSupplier(road));
    valueEncoder = new AvroValueEncoder(schemaVersion);
  }

  @Override
  public SchemaVersion getSchemaVersion() {
    Road road = getRoad();
    String roadName = road.getName();
    return SchemaVersion.latest(road.getSchemas().values()).orElseThrow(
        () -> new RoadUnavailableException(String.format("Road '%s' has no schema.", roadName)));
  }

  @Override
  protected Event<byte[], byte[]> encodeEvent(JsonNode jsonEvent, SchemaVersion schemaVersion)
    throws InvalidEventException {
    try {
      GenericRecord avroRecord = (GenericRecord) JasvornoConverter.convertToAvro(GenericData.get(), jsonEvent,
          schemaVersion.getSchema());
      byte[] key = keyEncoder.encode(jsonEvent);
      byte[] value = valueEncoder.encode(avroRecord);
      return new Event<>(key, value);
    } catch (JasvornoConverterException e) {
      metrics.markValidationFailures(getRoad().getName());
      throw new InvalidEventException(e.getMessage());
    }
  }

  @Override
  protected Future<Boolean> sendEncodedEvent(Event<byte[], byte[]> event, SchemaVersion schemaVersion)
    throws InvalidKeyException {
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(getRoad().getTopicName(), event.getKey(),
        event.getMessage());
    Future<RecordMetadata> future = kafkaProducer.send(record, this::updateMetrics);
    return Futures.lazyTransform(future, metadata -> true);
  }

  private void updateMetrics(RecordMetadata metadata, Exception e) {
    if (e == null) {
      metrics.markSuccessMetrics(getRoad().getName(), metadata.serializedValueSize());
    } else {
      metrics.markFailureMetrics(getRoad().getName());
    }
  }

  @VisibleForTesting
  static Supplier<Path> pathSupplier(Road road) {
    return () -> Optional
        .ofNullable(road.getPartitionPath())
        .filter(p -> !p.isEmpty())
        .map(KeyPathParser::parse)
        .orElse(null);
  }
}
