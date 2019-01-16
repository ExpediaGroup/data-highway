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
package com.hotels.road.schema.gdpr;

import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

import java.util.Collection;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

public class PiiLogicalTypeVisitor extends PiiVisitor<Void> {
  private static final LogicalType string = new LogicalType("pii-string");
  private static final LogicalType bytes = new LogicalType("pii-bytes");

  @Override
  protected void onPiiField(Field field, Collection<String> breadcrumb) {
    Schema schema = field.schema();
    if (schema.getType() == UNION) {
      for (Schema type : schema.getTypes()) {
        addLogicalType(type);
      }
    } else {
      addLogicalType(schema);
    }
  }

  private void addLogicalType(Schema schema) {
    if (schema.getType() == STRING) {
      string.addToSchema(schema);
    } else if (schema.getType() == BYTES) {
      bytes.addToSchema(schema);
    }
  }
}
