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

import java.time.Duration;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;

import com.hotels.road.offramp.model.Message;

/**
 * Offramp keeps track of the high-water mark or position in the stream that each consumer has reached. This allows
 * users to disconnect and return after interruptions with minimal re-processing. This class provides a simple mechanism
 * for consumers to notify the service that a given {@link Message} has been consumed and hence should be considered the
 * new high-water mark.
 */
@RequiredArgsConstructor(access = PACKAGE)
public class Committer {
  private final OfframpClient<?> client;
  private final Duration interval;
  private final DirectProcessor<Message<?>> processor;

  public static Committer create(OfframpClient<?> client, Duration interval) {
    return new Committer(client, interval, DirectProcessor.create());
  }

  public Mono<Void> start() {
    return Commits.fromMessages(processor, interval).flatMap(client::commit).doOnNext(success -> {
      if (!success) {
        throw new RuntimeException("Commit failed");
      }
    }).then();
  }

  /**
   * Saves the position in the stream at the given {@link Message Message's} partition and offset. Users should not
   * expect the status of individual messages to be tracked.
   *
   * @param m A {@link Message}.
   */
  public void commit(Message<?> m) {
    processor.onNext(m);
  }
}
