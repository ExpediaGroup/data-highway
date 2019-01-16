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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.road.schema.gdpr.PiiVisitor.PII;
import static com.hotels.road.schema.gdpr.PiiVisitor.SENSITIVITY;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import com.hotels.road.schema.gdpr.PiiLogicalTypeVisitor;

public class PiiLogicalTypeVisitorTest {
  private final PiiLogicalTypeVisitor underTest = new PiiLogicalTypeVisitor();

  @Test
  public void typicalString() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);

    underTest.onPiiField(field, null);

    assertThat(schema.getProp("logicalType"), is("pii-string"));
  }

  @Test
  public void typicalBytes() throws Exception {
    Schema schema = SchemaBuilder.builder().bytesType();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);

    underTest.onPiiField(field, null);

    assertThat(schema.getProp("logicalType"), is("pii-bytes"));
  }

  @Test
  public void typicalNullableString() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    Schema union = SchemaBuilder.unionOf().type(schema).and().nullType().endUnion();
    Field field = new Field("b", union, null, (Object) null);
    field.addProp(SENSITIVITY, PII);

    underTest.onPiiField(field, null);

    assertThat(schema.getProp("logicalType"), is("pii-string"));
  }

  @Test
  public void typicalNullableBytes() throws Exception {
    Schema schema = SchemaBuilder.builder().bytesType();
    Schema union = SchemaBuilder.unionOf().type(schema).and().nullType().endUnion();
    Field field = new Field("b", union, null, (Object) null);
    field.addProp(SENSITIVITY, PII);

    underTest.onPiiField(field, null);

    assertThat(schema.getProp("logicalType"), is("pii-bytes"));
  }
}
