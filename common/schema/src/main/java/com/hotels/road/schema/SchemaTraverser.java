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
package com.hotels.road.schema;

import static lombok.AccessLevel.PRIVATE;

import java.util.Collection;
import java.util.Stack;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import lombok.AllArgsConstructor;

import com.google.common.collect.ImmutableList;

@AllArgsConstructor(access = PRIVATE)
public final class SchemaTraverser {
  public static <T> T traverse(Schema schema, Visitor<T> visitor) {
    return traverse(schema, visitor, new Stack<>());
  }

  private static <T> T traverse(Schema schema, Visitor<T> visitor, Stack<String> breadcrumb) {
    visitor.onVisit(schema, ImmutableList.copyOf(breadcrumb));
    switch (schema.getType()) {
    case RECORD:
      for (Field field : schema.getFields()) {
        breadcrumb.push(field.name());
        visitor.onVisit(field, ImmutableList.copyOf(breadcrumb));
        traverse(field.schema(), visitor, breadcrumb);
        breadcrumb.pop();
      }
      break;
    case ARRAY:
      breadcrumb.push("*");
      traverse(schema.getElementType(), visitor, breadcrumb);
      breadcrumb.pop();
      break;
    case MAP:
      breadcrumb.push("*");
      traverse(schema.getValueType(), visitor, breadcrumb);
      breadcrumb.pop();
      break;
    case UNION:
      int i = 0;
      for (Schema type : schema.getTypes()) {
        breadcrumb.push(Integer.toString(i++));
        traverse(type, visitor, breadcrumb);
        breadcrumb.pop();
      }
      break;
    default:
      break;
    }
    if (breadcrumb.isEmpty()) {
      return visitor.getResult();
    }
    return null;
  }

  public interface Visitor<T> {
    void onVisit(Schema schema, Collection<String> breadcrumb);

    void onVisit(Field field, Collection<String> breadcrumb);

    T getResult();
  }
}
