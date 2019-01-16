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
package com.hotels.road.truck.park.decoder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.primitives.Ints;

@RunWith(MockitoJUnitRunner.class)
public class DataDeserializerTest {
  private static final String TOPIC = "topic";

  @Mock
  private SchemaLookup schemaLookup;

  private DataDeserializer underTest;

  @Before
  public void setUp() {
    underTest = new DataDeserializer(schemaLookup, GenericData.get());
  }

  @Test
  public void validSchemaAndData() {
    Schema schema = SchemaBuilder.record("r").fields().name("int8").type().intType().noDefault().endRecord();
    Record record = new Record(schema);
    record.put("int8", 2);
    int version = 1;

    when(schemaLookup.getSchema(1)).thenReturn(schema);

    byte[] value = toAvroBinary(schema, record, version);
    Record result = underTest.deserialize(TOPIC, value);

    assertThat(result.getSchema(), is(schema));
    assertThat(result, is(record));
  }

  @Test(expected = RuntimeException.class)
  public void incorrectMagicByte() {
    underTest.deserialize(TOPIC, new byte[] { 0x1 });
  }

  private byte[] toAvroBinary(Schema schema, Object value, int version) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      baos.write(0x0);
      baos.write(Ints.toByteArray(version));
      DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
      Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
      writer.write(value, encoder);
      encoder.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
