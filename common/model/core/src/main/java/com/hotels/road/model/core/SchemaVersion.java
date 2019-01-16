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
package com.hotels.road.model.core;

import java.util.Optional;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class SchemaVersion {
  private final Schema schema;
  private final int version;
  private final boolean deleted;

  @JsonCreator
  public SchemaVersion(
      @JsonProperty("schema") Schema schema,
      @JsonProperty("version") int version,
      @JsonProperty("deleted") boolean deleted) {
    this.schema = schema;
    this.version = version;
    this.deleted = deleted;
  }

  public static Optional<SchemaVersion> latest(Iterable<SchemaVersion> schemas) {
    return StreamSupport.stream(schemas.spliterator(), false).filter(s -> !s.isDeleted()).max(
        (s1, s2) -> s1.version - s2.version);
  }

  public static Optional<SchemaVersion> version(Iterable<SchemaVersion> schemas, int version) {
    return StreamSupport.stream(schemas.spliterator(), false).filter(s -> s.version == version).findFirst();
  }
}
