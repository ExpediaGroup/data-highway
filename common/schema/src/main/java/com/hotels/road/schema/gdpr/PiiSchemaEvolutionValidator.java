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

import static java.util.stream.Collectors.joining;

import static lombok.AccessLevel.PRIVATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import com.hotels.road.schema.SchemaTraverser;

@AllArgsConstructor(access = PRIVATE)
public final class PiiSchemaEvolutionValidator {
  public static void validate(Schema newSchema, Optional<Schema> currentSchema) {
    currentSchema.ifPresent(current -> {
      Fields fields = SchemaTraverser.traverse(current, new FieldsVisitor());
      SchemaTraverser.traverse(newSchema, new ValidatingVisitor(fields));
    });
  }

  static class FieldsVisitor extends PiiVisitor<Fields> {
    private final Fields fields = new Fields();

    @Override
    public void onVisit(Field field, Collection<String> breadcrumb) {
      super.onVisit(field, breadcrumb);
      fields.all.add(path(breadcrumb));
    }

    @Override
    protected void onPiiField(Field field, Collection<String> breadcrumb) {
      fields.pii.add(path(breadcrumb));
    }

    @Override
    public Fields getResult() {
      return fields;
    }
  }

  @RequiredArgsConstructor
  static class ValidatingVisitor extends PiiVisitor<Void> {
    private final Fields fields;

    @Override
    protected void onPiiField(Field field, Collection<String> breadcrumb) {
      String path = path(breadcrumb);
      // if the field was in the previous schema but it wasn't flagged as PII
      if (fields.all.contains(path) && !fields.pii.contains(path)) {
        throw new InvalidPiiAnnotationException(path);
      }
    }
  }

  static class Fields {
    private final List<String> all = new ArrayList<>();
    private final List<String> pii = new ArrayList<>();
  }

  private static String path(Collection<String> breadcrumb) {
    return breadcrumb.stream().collect(joining("/", "/", ""));
  }
}
