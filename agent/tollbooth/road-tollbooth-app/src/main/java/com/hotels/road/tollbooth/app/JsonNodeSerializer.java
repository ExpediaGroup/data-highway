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
package com.hotels.road.tollbooth.app;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.kafkastore.exceptions.SerializationException;
import com.hotels.road.kafkastore.serialization.Serializer;

@Component
public class JsonNodeSerializer implements Serializer<String, JsonNode> {
  private final ObjectMapper mapper;

  @Autowired
  public JsonNodeSerializer(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public String deserializeKey(byte[] key) throws SerializationException {
    return new String(key, UTF_8);
  }

  @Override
  public JsonNode deserializeValue(byte[] value) throws SerializationException {
    try {
      return mapper.readTree(value);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public byte[] serializeKey(String key) throws SerializationException {
    return key.getBytes(UTF_8);
  }

  @Override
  public byte[] serializeValue(JsonNode value) throws SerializationException {
    try {
      return mapper.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      throw new SerializationException(e);
    }
  }
}
