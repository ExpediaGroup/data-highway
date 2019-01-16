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
package com.hotels.road.truck.park.avro;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.road.truck.park.spi.RecordWriter;

public class AvroRecordWriter implements RecordWriter {
  private final DataFileWriter<Record> writer;

  AvroRecordWriter(Schema schema, OutputStream outputStream, CodecFactory codecFactory) throws IOException {
    writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    writer.setCodec(codecFactory);
    writer.create(schema, outputStream);
  }

  @Override
  public void write(Record record) throws IOException {
    writer.append(record);
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Component
  public static class Factory implements RecordWriter.Factory {
    private final CodecFactory codecFactory;

    @Autowired
    public Factory(CodecFactory codecFactory) {
      this.codecFactory = codecFactory;
    }

    @Override
    public RecordWriter create(Schema schema, OutputStream outputStream) throws IOException {
      return new AvroRecordWriter(schema, outputStream, codecFactory);
    }

  }
}
