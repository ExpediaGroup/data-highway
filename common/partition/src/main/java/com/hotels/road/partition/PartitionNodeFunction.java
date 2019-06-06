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
package com.hotels.road.partition;

import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.partition.KeyPathParser.Path;

/**
 * For a given {@link KeyPathParser.Path JsonPath}, returns the {@link JsonNode} fragment located within the provided
 * Json document.
 */
class PartitionNodeFunction implements Function<JsonNode, JsonNode> {

  private final JsonPointer pointer;

  PartitionNodeFunction(Path path) {
    // Currently KeyPathParser does not support '/' in names. However, escaping support has been added here in case that
    // changes.
    pointer = JsonPointer.compile(path
        .elements()
        .stream()
        .filter(e -> !e.isRoot())
        .map(e -> e.id())
        .map(s -> s.replaceAll("~", "~0"))
        .map(s -> s.replaceAll("/", "~1"))
        .collect(Collectors.joining("/", "/", "")));
  }

  @Override
  public JsonNode apply(JsonNode node) {
    return node.at(pointer);
  }

}
