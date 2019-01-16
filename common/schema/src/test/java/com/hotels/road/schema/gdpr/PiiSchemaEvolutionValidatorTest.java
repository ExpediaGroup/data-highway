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

import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

public class PiiSchemaEvolutionValidatorTest {
  @Test(expected = InvalidPiiAnnotationException.class)
  public void addPiiToExistingFieldFails() throws Exception {
    Schema newSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop("sensitivity", "PII")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    Schema currentSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    PiiSchemaEvolutionValidator.validate(newSchema, Optional.of(currentSchema));
  }

  @Test
  public void existingPiiNoChangeIsOk() throws Exception {
    Schema newSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop("sensitivity", "PII")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    Schema currentSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop("sensitivity", "PII")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    PiiSchemaEvolutionValidator.validate(newSchema, Optional.of(currentSchema));
  }

  @Test
  public void newSchemaIsOk() throws Exception {
    Schema newSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop("sensitivity", "PII")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    PiiSchemaEvolutionValidator.validate(newSchema, Optional.empty());
  }

  @Test
  public void newPiiFieldIsOk() throws Exception {
    Schema newSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop("sensitivity", "PII")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    Schema currentSchema = SchemaBuilder.record("r").fields().endRecord();

    PiiSchemaEvolutionValidator.validate(newSchema, Optional.of(currentSchema));
  }
}
