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
package com.hotels.road.schema.chronology;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class SchemaCompatibilityTest {

  private static final Schema STRING = SchemaBuilder.builder().stringType();
  private static final Schema NO_FIELDS = SchemaBuilder.record("r").fields().endRecord();
  private static final Schema STRING_NO_DEFAULT = SchemaBuilder
      .record("r")
      .fields()
      .name("f")
      .type(STRING)
      .noDefault()
      .endRecord();
  private static final Schema STRING_DEFAULT = SchemaBuilder
      .record("r")
      .fields()
      .name("f")
      .type(STRING)
      .withDefault("foo")
      .endRecord();

  @Test
  public void canReadAll_Ok() {
    SchemaCompatibility.CAN_READ_ALL.validate(STRING_NO_DEFAULT, existing(STRING_DEFAULT, STRING_DEFAULT));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void canReadAll_Fail() {
    SchemaCompatibility.CAN_READ_ALL.validate(STRING_NO_DEFAULT, existing(NO_FIELDS, STRING_DEFAULT));
  }

  @Test
  public void canReadLatest_Ok() {
    SchemaCompatibility.CAN_READ_LATEST.validate(STRING_NO_DEFAULT, existing(STRING_DEFAULT));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void canReadLatest_Fail() {
    SchemaCompatibility.CAN_READ_LATEST.validate(STRING_NO_DEFAULT, existing(NO_FIELDS));
  }

  @Test
  public void canBeReadByAll_Ok() {
    SchemaCompatibility.CAN_BE_READ_BY_ALL.validate(STRING_DEFAULT, existing(NO_FIELDS, STRING_NO_DEFAULT));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void canBeReadByAll_Fail() {
    SchemaCompatibility.CAN_BE_READ_BY_ALL.validate(STRING_NO_DEFAULT, existing(STRING));
  }

  @Test
  public void canBeReadByLatest_Ok() {
    SchemaCompatibility.CAN_BE_READ_BY_LATEST.validate(STRING_DEFAULT, existing(STRING, STRING_NO_DEFAULT));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void canBeReadByLatest_Fail() {
    SchemaCompatibility.CAN_BE_READ_BY_LATEST.validate(STRING_DEFAULT, existing(STRING));
  }

  @Test
  public void mutualAll_Ok() {
    SchemaCompatibility.MUTUAL_READ_ALL.validate(STRING_DEFAULT, existing(STRING_DEFAULT, STRING_DEFAULT));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void mutualAll_Fail() {
    SchemaCompatibility.MUTUAL_READ_ALL.validate(STRING_DEFAULT, existing(STRING, STRING_DEFAULT));
  }

  @Test
  public void mutualLatest_Ok() {
    SchemaCompatibility.MUTUAL_READ_LATEST.validate(STRING_DEFAULT, existing(STRING, STRING_DEFAULT));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void mutualLatest_Fail() {
    SchemaCompatibility.MUTUAL_READ_LATEST.validate(STRING_DEFAULT, existing(STRING));
  }

  private Map<Integer, Schema> existing(Schema... schemas) {
    Builder<Integer, Schema> builder = ImmutableMap.builder();
    int i = 0;
    for (Schema schema : schemas) {
      builder.put(i++, schema);
    }
    return builder.build();
  }

}
