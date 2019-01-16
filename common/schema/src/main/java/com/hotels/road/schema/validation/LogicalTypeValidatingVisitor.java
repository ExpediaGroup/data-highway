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
package com.hotels.road.schema.validation;

import static java.util.stream.Collectors.joining;

import java.util.Collection;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import com.hotels.road.schema.SchemaTraverser.Visitor;

public class LogicalTypeValidatingVisitor implements Visitor<Void> {
  @Override
  public void onVisit(Schema schema, Collection<String> breadcrumb) {
    try {
      LogicalTypes.fromSchema(schema);
    } catch (Exception e) {
      String path = breadcrumb.stream().collect(joining("/", "/", ""));
      throw new IllegalArgumentException("Invalid logical type declared at " + path, e);
    }
  }

  @Override
  public void onVisit(Field field, Collection<String> breadcrumb) {
    if (field.getProp(LogicalType.LOGICAL_TYPE_PROP) != null) {
      String path = breadcrumb.stream().collect(joining("/", "/", ""));
      throw new IllegalArgumentException("Invalid logical type declared on field at " + path);
    }
  }

  @Override
  public Void getResult() {
    return null;
  }
}
