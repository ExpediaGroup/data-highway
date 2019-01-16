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
package com.hotels.road.towtruck;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonNodeSerializerTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private final String key = "key";
  private final byte[] keyBytes = key.getBytes(UTF_8);

  private final JsonNode value = mapper.createObjectNode().put("foo", "bar");
  private final byte[] valueBytes = writeValueAsBytes(value);

  private JsonNodeSerializer underTest;

  @Before
  public void before() {
    underTest = new JsonNodeSerializer(mapper);
  }

  @Test
  public void deserializeKey() {
    String result = underTest.deserializeKey(keyBytes);

    assertThat(result, is(key));
  }

  @Test
  public void serializeKey() {
    byte[] result = underTest.serializeKey(key);

    assertThat(result, is(keyBytes));
  }

  @Test
  public void deserializeValue() {
    JsonNode result = underTest.deserializeValue(valueBytes);

    assertThat(result, is(value));
  }

  @Test
  public void serializeValue() {
    byte[] result = underTest.serializeValue(value);

    assertThat(result, is(valueBytes));
  }

  private byte[] writeValueAsBytes(JsonNode value) {
    try {
      return mapper.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
