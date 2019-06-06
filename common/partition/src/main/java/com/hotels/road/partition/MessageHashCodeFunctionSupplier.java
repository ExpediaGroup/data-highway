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

import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.partition.KeyPathParser.Path;

@RequiredArgsConstructor
public class MessageHashCodeFunctionSupplier implements Supplier<Function<JsonNode, Integer>>, AutoCloseable {

  private final @NonNull Supplier<Path> partitionPathSupplier;
  private final @NonNull Random random;

  public MessageHashCodeFunctionSupplier(Supplier<Path> partitionPathSupplier) {
    this(partitionPathSupplier, new Random());
  }

  /**
   * Constructs a function that returns the hash code of the supplied Json document. If a partition path is supplied and
   * this is valid within the context of the document, then the hash code is derived from the document fragment
   * described by the partition path. A random {@code int} is returned in all other cases.
   */
  @Override
  public Function<JsonNode, Integer> get() {
    return Optional
        .ofNullable(partitionPathSupplier.get())
        .map(path -> new PartitionNodeFunction(path).andThen(new JsonNodeHashCodeFunction(random)))
        .orElse(path -> random.nextInt());
  }

  @Override
  public void close() throws Exception {
    if (partitionPathSupplier instanceof AutoCloseable) {
      ((AutoCloseable) partitionPathSupplier).close();
    }
  }
}
