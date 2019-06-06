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
package com.hotels.road.offramp.model;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Test;

import lombok.Data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class EventTest {

  @Test
  public void request() throws Exception {
    ObjectMapper mapper = new ObjectMapper().registerModule(Event.module());
    Event event = new Request(1);
    String json = mapper.writeValueAsString(event);
    assertThat(json, is("{\"type\":\"REQUEST\",\"count\":1}"));
    Event result = mapper.readValue(json, Event.class);
    assertThat(result, is(event));
  }

  @Test
  public void commit() throws Exception {
    ObjectMapper mapper = new ObjectMapper().registerModule(Event.module());
    Map<Integer, Long> offsets = singletonMap(0, 1L);
    Event event = new Commit("correlationId", offsets);
    String json = mapper.writeValueAsString(event);
    Commit readValue = mapper.readValue(json, Commit.class);
    assertThat(readValue.getCorrelationId(), is("correlationId"));
    assertThat(readValue.getOffsets(), is(offsets));

    Event result = mapper.readValue(json, Event.class);
    assertThat(result, is(event));
  }

  @Test
  public void commitResponse() throws Exception {
    ObjectMapper mapper = new ObjectMapper().registerModule(Event.module());
    Event event = new CommitResponse("correlationId", true);
    String json = mapper.writeValueAsString(event);
    assertThat(json, is("{\"type\":\"COMMIT_RESPONSE\",\"correlationId\":\"correlationId\",\"success\":true}"));
    Event result = mapper.readValue(json, Event.class);
    assertThat(result, is(event));
  }

  @Test
  public void rebalance() throws Exception {
    ObjectMapper mapper = new ObjectMapper().registerModule(Event.module());
    Event event = new Rebalance(singleton(0));
    String json = mapper.writeValueAsString(event);
    assertThat(json, is("{\"type\":\"REBALANCE\",\"assignment\":[0]}"));
    Event result = mapper.readValue(json, Event.class);
    assertThat(result, is(event));
  }

  @Test
  public void connect() throws Exception {
    ObjectMapper mapper = new ObjectMapper().registerModule(Event.module());
    Event event = new Connection("foo");
    String json = mapper.writeValueAsString(event);
    assertThat(json, is("{\"type\":\"CONNECTION\",\"agentName\":\"foo\"}"));
    Event result = mapper.readValue(json, Event.class);
    assertThat(result, is(event));
  }

  @Test
  public void message() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TypeFactory typeFactory = mapper.getTypeFactory();
    mapper.registerModule(Event.module(typeFactory, typeFactory.constructType(Payload.class), Payload.class, null));
    Event event = new Message<>(0, 1L, 2, 3L, new Payload("bar"));
    String json = mapper.writeValueAsString(event);
    assertThat(json,
        is("{\"type\":\"MESSAGE\",\"partition\":0,\"offset\":1,\"schema\":2,\"timestampMs\":3,\"payload\":{\"foo\":\"bar\"}}"));
    Event result = mapper.readValue(json, Event.class);
    assertThat(result, is(event));
  }

  @Data
  static class Payload {
    private final String foo;
  }
}
