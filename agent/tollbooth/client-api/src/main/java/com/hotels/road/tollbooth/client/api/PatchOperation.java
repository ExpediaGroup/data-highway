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
package com.hotels.road.tollbooth.client.api;

import static com.hotels.road.tollbooth.client.api.Operation.ADD;
import static com.hotels.road.tollbooth.client.api.Operation.REMOVE;
import static com.hotels.road.tollbooth.client.api.Operation.REPLACE;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class PatchOperation {
  @JsonProperty("op")
  private final Operation operation;
  private final String path;
  private final Object value;

  public static PatchOperation add(String path, Object value) {
    return new PatchOperation(ADD, path, value);
  }

  public static PatchOperation remove(String path) {
    return new PatchOperation(REMOVE, path, null);
  }

  public static PatchOperation replace(String path, Object value) {
    return new PatchOperation(REPLACE, path, value);
  }

  @JsonCreator
  public PatchOperation(
      @JsonProperty("op") Operation operation,
      @JsonProperty("path") String path,
      @JsonProperty("value") Object value) {
    this.operation = operation;
    this.path = path;
    this.value = value;
  }
}
