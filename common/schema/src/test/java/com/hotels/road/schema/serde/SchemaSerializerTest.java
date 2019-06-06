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
package com.hotels.road.schema.serde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
public class SchemaSerializerTest {

  @Mock
  private JsonGenerator generator;

  private final Schema schema = SchemaBuilder
      .record("r")
      .fields()
      .name("f")
      .type()
      .booleanType()
      .noDefault()
      .endRecord();

  private JsonNode jsonNode;

  @Before
  public void before() throws JsonProcessingException, IOException {
    jsonNode = new ObjectMapper().readTree(schema.toString());
  }

  @Test
  public void test() throws JsonProcessingException, IOException {
    new SchemaSerializer().serialize(schema, generator, null);

    verify(generator).writeObject(jsonNode);
  }

  @Test
  public void handledType() {
    assertThat(new SchemaSerializer().handledType(), is(equalTo(Schema.class)));
  }

}
