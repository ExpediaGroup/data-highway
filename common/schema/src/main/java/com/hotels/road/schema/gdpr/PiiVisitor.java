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

import static java.util.stream.Collectors.toList;

import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.hotels.road.schema.SchemaTraverser.Visitor;

public class PiiVisitor<T> implements Visitor<T> {
  public static final String SENSITIVITY = "sensitivity";
  public static final String PII = "PII";

  @Override
  public void onVisit(Schema schema, Collection<String> breadcrumb) {
    if (PII.equalsIgnoreCase(schema.getProp(SENSITIVITY))) {
      throw new InvalidPiiAnnotationException(breadcrumb);
    }
  }

  @Override
  public void onVisit(Field field, Collection<String> breadcrumb) {
    if (PII.equalsIgnoreCase(field.getProp(SENSITIVITY))) {
      Schema schema = field.schema();
      if (!isStringOrBytes(schema) && !isNullableStringOrBytes(schema)) {
        throw new InvalidPiiAnnotationException(breadcrumb);
      }
      onPiiField(field, breadcrumb);
    }
  }

  @Override
  public T getResult() {
    return null;
  }

  /**
   * Override this for additional functionality when a PII field is found.
   */
  protected void onPiiField(Field field, Collection<String> breadcrumb) {}

  private static boolean isStringOrBytes(Schema schema) {
    Type type = schema.getType();
    return type == STRING || type == BYTES;
  }

  private static boolean isNullableStringOrBytes(Schema schema) {
    Type type = schema.getType();
    if (type == UNION) {
      List<Type> types = schema.getTypes().stream().map(Schema::getType).collect(toList());
      if (types.size() == 2 && types.contains(NULL) && (types.contains(STRING) || types.contains(BYTES))) {
        return true;
      }
    }
    return false;
  }
}
