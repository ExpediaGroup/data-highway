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
package com.hotels.road.schema.serde;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

public class SchemaSerializer extends JsonSerializer<Schema> {
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void serialize(Schema schema, JsonGenerator generator, SerializerProvider provider)
    throws IOException, JsonProcessingException {
    generator.writeObject(mapper.readTree(schema.toString()));
  }

  @Override
  public Class<Schema> handledType() {
    return Schema.class;
  }
}
