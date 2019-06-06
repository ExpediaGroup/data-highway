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
package com.hotels.road.onramp.kafka;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Test;

import com.hotels.road.model.core.SchemaVersion;

public class AvroValueEncoderTest {

  private final Schema schema = SchemaBuilder
      .builder()
      .record("r")
      .fields()
      .name("f")
      .type()
      .stringType()
      .noDefault()
      .endRecord();
  private final SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);

  private final AvroValueEncoder underTest = new AvroValueEncoder(schemaVersion);

  @Test
  public void encodeTwo() throws Exception {
    Record record = new Record(schema);
    record.put("f", "f1");
    byte[] result1 = underTest.encode(record);
    record.put("f", "f2");
    byte[] result2 = underTest.encode(record);

    assertRecord(result1, "f1");
    assertRecord(result2, "f2");
  }

  private void assertRecord(byte[] value, String expected) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(value);

    assertThat(buffer.get(), is((byte) 0));
    assertThat(buffer.getInt(), is(1));

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, buffer.position(), buffer.remaining(), null);
    Record read = new GenericDatumReader<Record>(schema).read(null, decoder);
    assertThat(read.get("f").toString(), is(expected));
  }
}
