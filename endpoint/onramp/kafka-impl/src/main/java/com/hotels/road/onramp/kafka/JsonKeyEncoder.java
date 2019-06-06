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
package com.hotels.road.onramp.kafka;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.road.partition.KeyPathParser.Path;
import com.hotels.road.partition.MessageHashCodeFunctionSupplier;

class JsonKeyEncoder {

  private final Supplier<Function<JsonNode, Integer>> hasherSupplier;

  JsonKeyEncoder(Supplier<Path> pathSupplier) {
    this(pathSupplier, new Random());
  }

  @VisibleForTesting
  JsonKeyEncoder(Supplier<Path> pathSupplier, Random random) {
    hasherSupplier = new MessageHashCodeFunctionSupplier(pathSupplier, random);
  }

  byte[] encode(final JsonNode record) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(hasherSupplier.get().apply(record)).array();
  }

}
