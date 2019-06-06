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
package com.hotels.road.client.partitioning;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.NonNull;

/**
 * A Function that given a message will delegate its execution to one of a supplied list of Functions using a supplied
 * hash function. Messages for which the hash function returns the same value will always be handled by the same
 * delegate.
 *
 * @param <MESSAGE> The Function's from type
 * @param <RESPONSE> The Function's to type
 */
public class MessagePartitioner<MESSAGE, RESPONSE> implements CloseableFunction<MESSAGE, RESPONSE> {
  private final Supplier<Function<MESSAGE, Integer>> hasherSupplier;
  private final List<Function<MESSAGE, RESPONSE>> delegates;

  public MessagePartitioner(
      @NonNull Supplier<Function<MESSAGE, Integer>> hasherSupplier,
      @NonNull List<Function<MESSAGE, RESPONSE>> delegates) {
    this.hasherSupplier = hasherSupplier;
    this.delegates = delegates;
    if (delegates.size() <= 0) {
      throw new IllegalArgumentException(getClass().getSimpleName() + " must have at least one delegate.");
    }
  }

  @Override
  public RESPONSE apply(MESSAGE message) {
    int partitionNumber = Math.abs(hasherSupplier.get().apply(message)) % delegates.size();
    return delegates.get(partitionNumber).apply(message);
  }

  @Override
  public void close() throws Exception {
    for (Function<MESSAGE, RESPONSE> delegate : delegates) {
      if (delegate instanceof AutoCloseable) {
        ((AutoCloseable) delegate).close();
      }
    }
  }
}
