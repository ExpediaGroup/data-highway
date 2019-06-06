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
package com.hotels.road.truck.park.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class DataDeserializer implements Deserializer<Record> {
  public static final byte MAGIC_BYTE = 0x0;

  private final SchemaLookup schemaLookup;
  private final GenericData genericData;

  @Override
  public Record deserialize(String topic, byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);
    if (buffer.get() != MAGIC_BYTE) {
      throw new RuntimeException("Unknown magic byte!");
    }
    int version = buffer.getInt();
    Schema schema = schemaLookup.getSchema(version);

    int offset = buffer.position();
    int length = buffer.remaining();

    DatumReader<Record> reader = new GenericDatumReader<>(schema, schema, genericData);
    Decoder decoder = DecoderFactory.get().binaryDecoder(data, offset, length, null);
    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException("Unable to decode record.", e);
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}
}
