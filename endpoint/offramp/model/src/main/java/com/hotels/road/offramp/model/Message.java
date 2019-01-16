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

import static com.hotels.road.offramp.model.Event.Type.MESSAGE;

import java.io.Serializable;

import lombok.Data;
import lombok.NonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class Message<T> implements Event, Serializable {
  private static final long serialVersionUID = 1L;
  private final Type type = MESSAGE;
  private final int partition;
  private final long offset;
  private final int schema;
  private final long timestampMs;
  private final @NonNull T payload;

  @JsonCreator
  public Message(
      @JsonProperty("partition") int partition,
      @JsonProperty("offset") long offset,
      @JsonProperty("schema") int schema,
      @JsonProperty("timestampMs") long timestampMs,
      @JsonProperty("payload") T payload) {
    this.partition = partition;
    this.offset = offset;
    this.schema = schema;
    this.timestampMs = timestampMs;
    this.payload = payload;
  }
}
