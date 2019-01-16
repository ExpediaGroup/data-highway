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
package com.hotels.road.schema.serde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
public class SchemaDeserializerTest {

  @Mock
  private JsonParser parser;

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
  public void deserialize() throws JsonProcessingException, IOException {
    when(parser.readValueAsTree()).thenReturn(jsonNode);

    Schema result = new SchemaDeserializer().deserialize(parser, null);

    assertThat(result, is(schema));
    verify(parser).readValueAsTree();
  }

  @Test(expected = AvroTypeException.class)
  public void detectsInvalidDefault() throws JsonProcessingException, IOException {
    jsonNode = new ObjectMapper().readTree(
        "{\"type\":\"record\",\"name\":\"mine\",\"fields\":[{\"name\":\"str\",\"type\":\"int\",\"default\":\"0\"}]}");
    // "0" is invalid for int, it should instead be 0

    when(parser.readValueAsTree()).thenReturn(jsonNode);

    new SchemaDeserializer().deserialize(parser, null);
  }

  @Test
  public void handledType() {
    assertThat(new SchemaDeserializer().handledType(), is(equalTo(Schema.class)));
  }

}
