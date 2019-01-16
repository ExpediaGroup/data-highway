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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.road.schema.SchemaTraverser;
import com.hotels.road.schema.SchemaTraverser.Visitor;

@RunWith(MockitoJUnitRunner.class)
public class SchemaTraverserTest {

  private @Mock Visitor<String> visitor;

  @Test
  public void visitorResult() throws Exception {
    doReturn("result").when(visitor).getResult();
    Schema schema = SchemaBuilder.builder().intType();

    String result = SchemaTraverser.traverse(schema, visitor);

    assertThat(result, is("result"));
  }

  @Test
  public void testName() throws Exception {
    Schema intType = SchemaBuilder.builder().intType();
    Schema stringType = SchemaBuilder.builder().stringType();
    Schema booleanType = SchemaBuilder.builder().booleanType();
    Schema longType = SchemaBuilder.builder().longType();
    Schema nullType = SchemaBuilder.builder().nullType();
    Schema mapType = SchemaBuilder.map().values(stringType);
    Schema arrayType = SchemaBuilder.array().items(booleanType);
    Schema unionType = SchemaBuilder.unionOf().type(longType).and().type(nullType).endUnion();
    Schema recordType = SchemaBuilder
        .record("r")
        .fields()
        .name("f1")
        .type(intType)
        .noDefault()
        .name("f2")
        .type(mapType)
        .noDefault()
        .name("f3")
        .type(arrayType)
        .noDefault()
        .name("f4")
        .type(unionType)
        .noDefault()
        .endRecord();

    SchemaTraverser.traverse(recordType, visitor);

    InOrder inOrder = inOrder(visitor);

    inOrder.verify(visitor).onVisit(eq(recordType), eq(ImmutableList.of()));

    Field field;
    field = new Field("f1", intType, null, (Object) null);
    inOrder.verify(visitor).onVisit(eq(field), eq(ImmutableList.of("f1")));
    inOrder.verify(visitor).onVisit(eq(intType), eq(ImmutableList.of("f1")));

    field = new Field("f2", mapType, null, (Object) null);
    inOrder.verify(visitor).onVisit(eq(field), eq(ImmutableList.of("f2")));
    inOrder.verify(visitor).onVisit(eq(mapType), eq(ImmutableList.of("f2")));
    inOrder.verify(visitor).onVisit(eq(stringType), eq(ImmutableList.of("f2", "*")));

    field = new Field("f3", arrayType, null, (Object) null);
    inOrder.verify(visitor).onVisit(eq(field), eq(ImmutableList.of("f3")));
    inOrder.verify(visitor).onVisit(eq(arrayType), eq(ImmutableList.of("f3")));
    inOrder.verify(visitor).onVisit(eq(booleanType), eq(ImmutableList.of("f3", "*")));

    field = new Field("f4", unionType, null, (Object) null);
    inOrder.verify(visitor).onVisit(eq(field), eq(ImmutableList.of("f4")));
    inOrder.verify(visitor).onVisit(eq(unionType), eq(ImmutableList.of("f4")));
    inOrder.verify(visitor).onVisit(eq(longType), eq(ImmutableList.of("f4", "0")));
    inOrder.verify(visitor).onVisit(eq(nullType), eq(ImmutableList.of("f4", "1")));

    inOrder.verify(visitor).getResult();
    inOrder.verifyNoMoreInteractions();
  }
}
