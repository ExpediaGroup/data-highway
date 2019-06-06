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
package com.hotels.road.model.kafka;

import static java.util.Collections.singletonMap;

import static com.fasterxml.jackson.core.Version.unknownVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.OffsetDateTime;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.road.agents.trafficcop.spi.ModelReader;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.serde.ObjectMapperFactory;
import com.hotels.road.schema.serde.SchemaSerializationModule;

@Component
public class RoadReader implements ModelReader<Road> {
  private final ObjectReader reader;

  public RoadReader() {
    this(new ObjectMapperFactory().newInstance().registerModule(new SchemaSerializationModule()).registerModule(offsetDateTimeModule()));
  }

  @VisibleForTesting
  RoadReader(ObjectMapper objectMapper) {
    reader = objectMapper.readerFor(Road.class);
  }

  @Override
  public Road read(JsonNode json) {
    try {
      return reader.readValue(json);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Module offsetDateTimeModule() {
    return new SimpleModule("OffsetDateTime", unknownVersion(),
        singletonMap(OffsetDateTime.class, new JsonDeserializer<OffsetDateTime>() {
          @Override
          public OffsetDateTime deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
            return OffsetDateTime.parse(p.getValueAsString());
          }
        }));
  }
}
