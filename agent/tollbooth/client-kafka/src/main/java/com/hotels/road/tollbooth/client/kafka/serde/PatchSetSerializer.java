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
package com.hotels.road.tollbooth.client.kafka.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.hotels.road.tollbooth.client.api.PatchSet;

public class PatchSetSerializer implements Serializer<PatchSet> {
  private final ObjectWriter objectWriter;

  /**
   * Required for configs where only class names are used to instantiate PatchSetSerializer.
   */
  public PatchSetSerializer() {
    this(new ObjectMapper().writer());
  }

  public PatchSetSerializer(ObjectWriter objectWriter) {
    this.objectWriter = objectWriter;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, PatchSet patch) {
    try {
      return objectWriter.writeValueAsBytes(patch);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {}
}
