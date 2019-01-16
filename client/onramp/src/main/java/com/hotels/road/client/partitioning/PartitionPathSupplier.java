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
package com.hotels.road.client.partitioning;

import java.util.Optional;
import java.util.function.Supplier;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;

/** Supplies a parsed partition path from the road model, returning null if the path is missing, null, or empty. */
@RequiredArgsConstructor
public class PartitionPathSupplier implements Supplier<Path>, AutoCloseable {

  private final @NonNull Supplier<JsonNode> roadModelSupplier;

  @Override
  public Path get() {
    return Optional
        .of(roadModelSupplier.get().path("partitionPath"))
        .filter(path -> !path.isMissingNode())
        .filter(path -> !path.isNull())
        .map(JsonNode::asText)
        .filter(text -> !text.isEmpty())
        .map(KeyPathParser::parse)
        .orElse(null);
  }

  @Override
  public void close() throws Exception {
    if (roadModelSupplier instanceof AutoCloseable) {
      ((AutoCloseable) roadModelSupplier).close();
    }
  }
}
