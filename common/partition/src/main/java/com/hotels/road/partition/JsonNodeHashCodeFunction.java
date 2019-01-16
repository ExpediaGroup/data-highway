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
package com.hotels.road.partition;

import static lombok.AccessLevel.PACKAGE;

import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

/**
 * Returns a random {@code int} if {@code null}, or a {@link JsonNodeType#MISSING MISSING} node was provided; the node's
 * hash code otherwise.
 */
@RequiredArgsConstructor(access = PACKAGE)
class JsonNodeHashCodeFunction implements Function<JsonNode, Integer> {

  private final @NonNull Random random;

  @Override
  public Integer apply(JsonNode node) {
    return Optional.of(node).filter(n -> !n.isMissingNode()).map(Objects::hashCode).orElse(random.nextInt());
  }

}
