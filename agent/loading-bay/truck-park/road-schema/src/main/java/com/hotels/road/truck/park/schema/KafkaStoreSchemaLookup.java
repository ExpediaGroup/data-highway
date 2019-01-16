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
package com.hotels.road.truck.park.schema;

import java.util.Map;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import com.hotels.road.truck.park.decoder.SchemaLookup;

@Component
public class KafkaStoreSchemaLookup implements SchemaLookup {

  private final Map<String, Road> roads;
  private final String roadName;

  @Autowired
  KafkaStoreSchemaLookup(Map<String, Road> roads, @Value("${road.name}") String roadName) {
    this.roadName = roadName;
    this.roads = roads;
  }

  @Override
  public Schema getSchema(int version) {
    return roads.get(roadName).getSchemas().get(version).getSchema();
  }

  @JsonDeserialize(builder = KafkaStoreSchemaLookup.SchemaVersion.SchemaVersionBuilder.class)
  @lombok.Data
  @lombok.Builder
  @lombok.RequiredArgsConstructor
  public static class SchemaVersion {
    private final Schema schema;
    private final int version;
    private final boolean deleted;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPOJOBuilder(withPrefix = "")
    public static final class SchemaVersionBuilder {}
  }

  @JsonDeserialize(builder = KafkaStoreSchemaLookup.Road.RoadBuilder.class)
  @lombok.Data
  @lombok.Builder
  public static class Road {
    private final Map<Integer, SchemaVersion> schemas;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPOJOBuilder(withPrefix = "")
    public static final class RoadBuilder {}
  }

}
