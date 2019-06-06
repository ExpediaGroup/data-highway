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
package com.hotels.road.partition;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.function.Function;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;

public class PartitionNodeFunctionTest {

  private Function<JsonNode, JsonNode> underTest;

  @Test
  public void valueNode() throws JsonProcessingException, IOException {
    String json = "{ \"a\": { \"b\": { \"c\": 1 } } }";
    JsonNode node = new ObjectMapper().readTree(json);
    underTest = new PartitionNodeFunction(KeyPathParser.parse("$.a.b.c"));

    JsonNode result = underTest.apply(node);

    assertThat(result, is(JsonNodeFactory.instance.numberNode(1)));
  }

  @Test
  public void objectNode() throws JsonProcessingException, IOException {
    String json = "{ \"a\": { \"b\": { \"c\": 1 } } }";
    JsonNode node = new ObjectMapper().readTree(json);
    underTest = new PartitionNodeFunction(KeyPathParser.parse("$.a.b"));

    JsonNode result = underTest.apply(node);

    assertThat(result, is(JsonNodeFactory.instance.objectNode().put("c", 1)));
  }

  @Test
  public void missingNode() throws JsonProcessingException, IOException {
    String json = "{ \"a\": { \"b\": {} } }";
    JsonNode node = new ObjectMapper().readTree(json);
    underTest = new PartitionNodeFunction(KeyPathParser.parse("$.a.b.c"));

    JsonNode result = underTest.apply(node);

    assertThat(result, is(MissingNode.getInstance()));
  }

  @Test
  public void nullNode() throws JsonProcessingException, IOException {
    String json = "{ \"a\": { \"b\": { \"c\": null } } }";
    JsonNode node = new ObjectMapper().readTree(json);
    underTest = new PartitionNodeFunction(KeyPathParser.parse("$.a.b.c"));

    JsonNode result = underTest.apply(node);

    assertThat(result, is(NullNode.getInstance()));
  }

}
