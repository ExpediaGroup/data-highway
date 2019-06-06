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
import org.apache.avro.Schema.Parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class SchemaDeserializer extends JsonDeserializer<Schema> {
  @Override
  public Schema deserialize(JsonParser parser, DeserializationContext context)
    throws IOException, JsonProcessingException {
    Parser schemaParser = new Schema.Parser();
    // Validate any default values provided
    schemaParser.setValidateDefaults(true);
    // Validate all names.
    schemaParser.setValidate(true);
    return schemaParser.parse(parser.readValueAsTree().toString());
  }

  @Override
  public Class<?> handledType() {
    return Schema.class;
  }
}
