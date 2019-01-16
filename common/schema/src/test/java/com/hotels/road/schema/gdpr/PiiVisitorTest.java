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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static com.hotels.road.schema.gdpr.PiiVisitor.PII;
import static com.hotels.road.schema.gdpr.PiiVisitor.SENSITIVITY;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.road.schema.gdpr.PiiVisitor;

@RunWith(MockitoJUnitRunner.class)
public class PiiVisitorTest {
  private final Collection<String> breadcrumb = ImmutableList.of("a", "b");
  private PiiVisitor<Void> underTest;

  @Before
  public void before() throws Exception {
    underTest = spy(new PiiVisitor<>());
  }

  @Test
  public void getResult() throws Exception {
    assertThat(underTest.getResult(), is(nullValue()));
  }

  @Test
  public void schemaWithPii() throws Exception {
    Schema schema = SchemaBuilder.builder().stringBuilder().prop(SENSITIVITY, PII).endString();
    try {
      underTest.onVisit(schema, breadcrumb);
      fail();
    } catch (InvalidPiiAnnotationException e) {
      assertThat(e.getPath(), is("/a/b"));
    }
  }

  @Test
  public void schemaWithoutPii() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    underTest.onVisit(schema, breadcrumb);
  }

  @Test
  public void fieldWithInvalidPii() throws Exception {
    Schema schema = SchemaBuilder.builder().intType();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    try {
      underTest.onVisit(field, breadcrumb);
      fail();
    } catch (InvalidPiiAnnotationException e) {
      assertThat(e.getPath(), is("/a/b"));
    }
    verify(underTest, never()).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithNoPii() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    Field field = new Field("b", schema, null, (Object) null);
    underTest.onVisit(field, breadcrumb);
    verify(underTest, never()).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithStringPii() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    underTest.onVisit(field, breadcrumb);
    verify(underTest).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithBytesPii() throws Exception {
    Schema schema = SchemaBuilder.builder().bytesType();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    underTest.onVisit(field, breadcrumb);
    verify(underTest).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithNullableStringPii() throws Exception {
    Schema schema = SchemaBuilder.unionOf().stringType().and().nullType().endUnion();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    underTest.onVisit(field, breadcrumb);
    verify(underTest).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithNullableBytesPii() throws Exception {
    Schema schema = SchemaBuilder.unionOf().bytesType().and().nullType().endUnion();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    underTest.onVisit(field, breadcrumb);
    verify(underTest).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithPiiUnion3Types() throws Exception {
    Schema schema = SchemaBuilder.unionOf().intType().and().booleanType().and().floatType().endUnion();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    try {
      underTest.onVisit(field, breadcrumb);
      fail();
    } catch (InvalidPiiAnnotationException e) {
      assertThat(e.getPath(), is("/a/b"));
    }
    verify(underTest, never()).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithPiiUnionNotNullable() throws Exception {
    Schema schema = SchemaBuilder.unionOf().intType().and().booleanType().endUnion();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    try {
      underTest.onVisit(field, breadcrumb);
      fail();
    } catch (InvalidPiiAnnotationException e) {
      assertThat(e.getPath(), is("/a/b"));
    }
    verify(underTest, never()).onPiiField(field, breadcrumb);
  }

  @Test
  public void fieldWithPiiNullableInt() throws Exception {
    Schema schema = SchemaBuilder.unionOf().intType().and().nullType().endUnion();
    Field field = new Field("b", schema, null, (Object) null);
    field.addProp(SENSITIVITY, PII);
    try {
      underTest.onVisit(field, breadcrumb);
      fail();
    } catch (InvalidPiiAnnotationException e) {
      assertThat(e.getPath(), is("/a/b"));
    }
    verify(underTest, never()).onPiiField(field, breadcrumb);
  }
}
