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

import static lombok.AccessLevel.PRIVATE;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import reactor.core.publisher.Flux;

import com.hotels.road.offramp.model.Message;

@AllArgsConstructor(access = PRIVATE)
public final class Commits {
  public static Flux<Map<Integer, Long>> fromMessages(@NonNull Flux<Message<?>> messages, @NonNull Duration interval) {
    if (interval.toMillis() < 0L) {
      throw new IllegalArgumentException("Must not be a negative interval.");
    }
    return messages
        .window(interval)
        .<Map<Integer, Long>> flatMap(
            f -> f.collect(HashMap::new, (c, m) -> c.merge(m.getPartition(), m.getOffset() + 1L, Math::max)))
        .filter(x -> !x.isEmpty());
  }
}
