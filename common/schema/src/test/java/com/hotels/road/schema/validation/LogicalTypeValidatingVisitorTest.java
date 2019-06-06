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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.Collections;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

public class LogicalTypeValidatingVisitorTest {

  private final LogicalTypeValidatingVisitor underTest = new LogicalTypeValidatingVisitor();
  private final Collection<String> breadcrumb = Collections.singleton("path");

  @Test(expected = IllegalArgumentException.class)
  public void invalidLogicalType() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    schema.addProp(LogicalType.LOGICAL_TYPE_PROP, "date");
    underTest.onVisit(schema, breadcrumb);
  }

  @Test
  public void validLogicalType() throws Exception {
    Schema schema = SchemaBuilder.builder().intType();
    schema.addProp(LogicalType.LOGICAL_TYPE_PROP, "date");
    underTest.onVisit(schema, breadcrumb);
  }

  @Test(expected = IllegalArgumentException.class)
  public void logicalTypeField() throws Exception {
    Field field = new Field("f", SchemaBuilder.builder().intType(), null, (String) null);
    field.addProp(LogicalType.LOGICAL_TYPE_PROP, "date");
    underTest.onVisit(field, breadcrumb);
  }

  @Test
  public void noLogicalTypeField() throws Exception {
    Field field = new Field("f", SchemaBuilder.builder().intType(), null, (String) null);
    underTest.onVisit(field, breadcrumb);
  }

  @Test
  public void result() {
    assertThat(underTest.getResult(), is(nullValue()));
  }
}
