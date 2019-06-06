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
package com.hotels.road.offramp.client;

import java.util.Map;
import java.util.Set;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;

import com.hotels.road.offramp.model.Message;

/**
 * Interface for consuming data from a Data Highway road.
 */
public interface OfframpClient<T> extends AutoCloseable {
  static <T> OfframpClient<T> create(OfframpOptions<T> options) {
    return new OfframpClientImpl<>(options);
  }

  /**
   * @return A {@link Publisher} of {@link Message Messages}
   */
  Publisher<Message<T>> messages();

  /**
   * Commits offsets. Note that the offset to be committed for any given partition is the next offset that the user
   * wishes to receive, not the last consumed offset. For example, if offset 10 has been consumed, then offset 11 should
   * be committed.
   *
   * @param offsets A {@link Map} of partition offsets to be committed.
   * @return A {@link Mono} to allow user to control execution.
   */
  Mono<Boolean> commit(Map<Integer, Long> offsets);

  /**
   * @return A {@link Publisher} of currently assigned partitions
   */
  Publisher<Set<Integer>> rebalances();
}
