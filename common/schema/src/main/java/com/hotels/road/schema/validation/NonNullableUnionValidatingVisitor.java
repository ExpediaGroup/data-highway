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
package com.hotels.road.schema.validation;

import static java.util.stream.Collectors.joining;

import static org.apache.avro.Schema.Type.NULL;

import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.hotels.road.schema.SchemaTraverser.Visitor;

public class NonNullableUnionValidatingVisitor implements Visitor<Void> {
  @Override
  public void onVisit(Schema schema, Collection<String> breadcrumb) {
    if (Type.UNION == schema.getType()) {
      List<Schema> types = schema.getTypes();
      if (types.size() != 2 || (types.get(0).getType() != NULL && types.get(1).getType() != NULL)) {
        String path = breadcrumb.stream().collect(joining("/", "/", ""));
        throw new IllegalArgumentException(String
            .format("Only union[any, null] or union[null, any] are supported. At path %s got %s", path, schema));
      }
    }
  }

  @Override
  public void onVisit(Field field, Collection<String> breadcrumb) {}

  @Override
  public Void getResult() {
    return null;
  }
}
