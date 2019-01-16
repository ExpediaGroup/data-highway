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
package com.hotels.road.truck.park.decoder.gdpr;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import static com.hotels.road.schema.gdpr.PiiVisitor.PII;
import static com.hotels.road.schema.gdpr.PiiVisitor.SENSITIVITY;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.truck.park.decoder.SchemaLookup;

@RunWith(MockitoJUnitRunner.class)
public class PiiLogicalTypeSchemaLookupTest {
  private @Mock SchemaLookup delegate;

  private PiiLogicalTypeSchemaLookup underTest;

  @Before
  public void before() throws Exception {
    underTest = new PiiLogicalTypeSchemaLookup(delegate);
  }

  @Test
  public void piiStringField() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop(SENSITIVITY, PII)
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    doReturn(schema).when(delegate).getSchema(1);

    Schema result = underTest.getSchema(1);

    assertThat(result.getField("f").schema().getProp("logicalType"), is("pii-string"));
  }
}
