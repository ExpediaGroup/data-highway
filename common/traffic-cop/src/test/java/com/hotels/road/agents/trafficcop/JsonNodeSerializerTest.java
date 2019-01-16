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
package com.hotels.road.agents.trafficcop;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import lombok.Data;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.kafkastore.exceptions.SerializationException;

public class JsonNodeSerializerTest {
  @Data
  private static class TestObject {
    private final String foo;
  }

  private final ObjectMapper mapper = new ObjectMapper();
  private final ModelSerializer<TestObject> serializer = new ModelSerializer<>(mapper,
      json -> mapper.convertValue(json, TestObject.class));

  private final byte[] jsonBytes = "{\"foo\":\"bar\"}".getBytes(UTF_8);

  @Test
  public void deserializeKeyTest() throws Exception {
    assertThat(serializer.deserializeKey(new byte[] { 72, 101, 108, 108, 111 }), is("Hello"));
  }

  @Test
  public void deserializeValueTest() throws Exception {
    assertThat(serializer.deserializeValue(jsonBytes), is(new TestObject("bar")));
  }

  @Test(expected = SerializationException.class)
  public void deserializeValue_fail_with_bad_json_Test() throws Exception {
    byte[] bytes = "{\"bad json\"}".getBytes(UTF_8);
    serializer.deserializeValue(bytes);
  }
}
