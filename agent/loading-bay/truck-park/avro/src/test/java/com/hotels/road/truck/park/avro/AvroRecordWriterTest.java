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
package com.hotels.road.truck.park.avro;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hotels.road.truck.park.avro.AvroRecordWriter.Factory;
import com.hotels.road.truck.park.spi.RecordWriter;

public class AvroRecordWriterTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void typical() throws Exception {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .requiredString("name")
        .endRecord();
    Record value = new GenericRecordBuilder(schema).set("id", 1L).set("name", "hello").build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    Factory factory = new Factory(CodecFactory.nullCodec());
    RecordWriter writer = factory.create(schema, output);
    writer.write(value);
    writer.close();

    SeekableInput input = new SeekableByteArrayInput(output.toByteArray());
    DatumReader<Record> datumReader = new GenericDatumReader<>(schema);
    DataFileReader<Record> dataFileReader = new DataFileReader<>(input, datumReader);
    assertThat(dataFileReader.next(), is(value));
    assertThat(dataFileReader.hasNext(), is(false));
    dataFileReader.close();
  }

}
