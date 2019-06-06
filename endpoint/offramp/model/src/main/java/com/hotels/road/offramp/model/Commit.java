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

import static java.util.UUID.randomUUID;

import static com.hotels.road.offramp.model.Event.Type.COMMIT;

import java.util.Map;

import lombok.Data;
import lombok.NonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class Commit implements Event {
  private final Type type = COMMIT;
  private final @NonNull String correlationId;
  private final @NonNull Map<Integer, Long> offsets;

  public Commit(Map<Integer, Long> offsets) {
    this(randomUUID().toString(), offsets);
  }

  @JsonCreator
  public Commit(
      @JsonProperty("correlationId") String correlationId,
      @JsonProperty("offsets") Map<Integer, Long> offsets) {
    this.correlationId = correlationId;
    this.offsets = offsets;
  }
}
