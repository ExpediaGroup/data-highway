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
package com.hotels.road.offramp.model;

import static java.util.Collections.singletonMap;

import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.Version.unknownVersion;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;

@JsonPropertyOrder("type") // Ensure type is first to allow deserializers to peek
public interface Event {
  Type getType();

  public enum Type {
    MESSAGE,
    REQUEST,
    CANCEL,
    COMMIT,
    COMMIT_RESPONSE,
    REBALANCE,
    CONNECTION;
  }

  public static <T> Module module(
      @NonNull TypeFactory typeFactory,
      @NonNull JavaType payloadType,
      Class<T> payloadClass,
      JsonDeserializer<T> payloadDeserializer) {
    Map<Class<?>, JsonDeserializer<?>> deserializers = new HashMap<>();
    JavaType messageType = typeFactory.constructParametricType(Message.class, payloadType);
    deserializers.put(Event.class, new Event.Deserialiser(messageType));
    if (payloadClass != null && payloadDeserializer != null) {
      deserializers.put(payloadClass, payloadDeserializer);
    }
    return new SimpleModule("event", unknownVersion(), deserializers);
  }

  public static <T> Module module() {
    return new SimpleModule("event", unknownVersion(), singletonMap(Event.class, new Event.Deserialiser(null)));
  }

  @RequiredArgsConstructor
  static class Deserialiser extends JsonDeserializer<Event> {
    private final JavaType messageType;

    @SuppressWarnings("deprecation")
    @Override
    public Event deserialize(JsonParser parser, DeserializationContext context)
      throws IOException, JsonProcessingException {
      // read the type so we know what to deserialise to
      expect(parser.getCurrentToken(), START_OBJECT, context);
      expect(parser.nextToken(), FIELD_NAME, context);
      if (!"type".equals(parser.getCurrentName())) {
        context.mappingException("Expected \"type\" field but got %s", parser.getCurrentName());
      }
      expect(parser.nextToken(), VALUE_STRING, context);
      Type type = Type.valueOf(parser.getText());

      // shift to the next FIELD_NAME so the context can deserialize the rest
      parser.nextToken();

      switch (type) {
      case MESSAGE:
        return context.readValue(parser, messageType);
      case REQUEST:
        return context.readValue(parser, Request.class);
      case CANCEL:
        return context.readValue(parser, Cancel.class);
      case COMMIT:
        return context.readValue(parser, Commit.class);
      case COMMIT_RESPONSE:
        return context.readValue(parser, CommitResponse.class);
      case REBALANCE:
        return context.readValue(parser, Rebalance.class);
      case CONNECTION:
        return context.readValue(parser, Connection.class);
      default:
        throw new RuntimeException("Unknown Event Type: " + type);
      }
    }

    @SuppressWarnings("deprecation")
    private void expect(JsonToken token, JsonToken expected, DeserializationContext context)
      throws JsonMappingException {
      if (!token.equals(expected)) {
        context.mappingException("Expected JsonToken %s but got %s", expected, token);
      }
    }
  }
}
