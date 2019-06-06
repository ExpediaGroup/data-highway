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

import static lombok.AccessLevel.PACKAGE;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

import com.hotels.road.offramp.model.Commit;
import com.hotels.road.offramp.model.CommitResponse;

@RequiredArgsConstructor(access = PACKAGE)
class CommitHandler {
  private final Map<String, CompletableFuture<Boolean>> commits = new HashMap<>();
  private final EventSender eventSender;

  Mono<Boolean> commit(Map<Integer, Long> offsets) {
    Commit commit = createCommit(offsets);
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    synchronized (commits) {
      commits.put(commit.getCorrelationId(), result);
    }
    return Mono.just(commit).doOnNext(eventSender::send).flatMap(c -> Mono.fromFuture(result));
  }

  void complete(CommitResponse response) {
    CompletableFuture<Boolean> result;
    synchronized (commits) {
      result = commits.remove(response.getCorrelationId());
    }
    if (result != null) {
      result.complete(response.isSuccess());
    } else {
      throw new IllegalStateException("Received response for unknown commit: " + response);
    }
  }

  void onClose() {
    synchronized (commits) {
      commits.forEach((correlationId, result) -> result.complete(false));
      commits.clear();
    }
  }

  Commit createCommit(Map<Integer, Long> offsets) {
    return new Commit(offsets);
  }

  interface Factory {
    CommitHandler create(EventSender eventSender);
  }
}
