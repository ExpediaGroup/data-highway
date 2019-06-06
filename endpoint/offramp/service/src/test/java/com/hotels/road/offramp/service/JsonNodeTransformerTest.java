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
package com.hotels.road.offramp.service;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

@RunWith(MockitoJUnitRunner.class)
public class JsonNodeTransformerTest {
  private @Mock Function<JsonNode, JsonNode> obfuscator;
  private JsonNodeTransformer underTest;

  private final ObjectMapper mapper = new ObjectMapper();
  private final ObjectNode f1Unnested = mapper.createObjectNode().put("f1", "foo");
  private final ObjectNode f2Unnested = mapper.createObjectNode().put("f2", "foo");
  private final ObjectNode nested = nested();
  private final ArrayNode array = array();

  @Before
  public void before() throws Exception {
    underTest = new JsonNodeTransformer(obfuscator);
  }

  @Test
  public void unnestedValue() throws Exception {
    doReturn(TextNode.valueOf("bar")).when(obfuscator).apply(TextNode.valueOf("foo"));

    JsonNode result = underTest.transform(f1Unnested, singletonList("/f1"));

    assertThat(result, is(mapper.createObjectNode().put("f1", "bar")));
  }

  @Test
  public void nestedValue() throws Exception {
    doReturn(TextNode.valueOf("bar")).when(obfuscator).apply(TextNode.valueOf("foo"));

    JsonNode result = underTest.transform(nested, singletonList("/f1/f2"));

    assertThat(result.get("f1"), is(mapper.createObjectNode().put("f2", "bar")));
  }

  @Test
  public void mapValue() throws Exception {
    doReturn(TextNode.valueOf("bar")).when(obfuscator).apply(TextNode.valueOf("foo"));

    JsonNode result = underTest.transform(nested, singletonList("/*/f2"));

    assertThat(result.get("f1"), is(mapper.createObjectNode().put("f2", "bar")));
  }

  @Test
  public void arrayValue() throws Exception {
    doReturn(TextNode.valueOf("bar")).when(obfuscator).apply(TextNode.valueOf("foo"));

    JsonNode result = underTest.transform(array, singletonList("/*/f2"));

    assertThat(result.get(0), is(mapper.createObjectNode().put("f2", "bar")));
  }

  @Test
  public void unionValue() throws Exception {
    doReturn(TextNode.valueOf("bar")).when(obfuscator).apply(TextNode.valueOf("foo"));

    JsonNode result = underTest.transform(f1Unnested, singletonList("/0/f1"));

    assertThat(result, is(mapper.createObjectNode().put("f1", "bar")));
  }

  @Test
  public void missingParent() throws Exception {
    underTest.transform(nested, singletonList("/missing/f2"));
    assertThat(nested.get("missing/f2"), is(nullValue()));
  }

  @Test
  public void missingField() throws Exception {
    underTest.transform(nested, singletonList("/f1/missing"));
    assertThat(nested.get("missing"), is(nullValue()));
  }

  private ObjectNode nested() {
    ObjectNode node = mapper.createObjectNode();
    node.set("f1", f2Unnested);
    return node;
  }

  private ArrayNode array() {
    ArrayNode node = mapper.createArrayNode();
    node.add(f2Unnested);
    return node;
  }
}
