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
package com.hotels.road.weighbridge.function;

import java.util.function.Predicate;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Component
@Slf4j
@RequiredArgsConstructor
public class BrokerNodeFunction {
  private final AdminClient client;

  public BrokerNode apply(Predicate<String> hostPredicate) {
    return Mono
        .fromSupplier(() -> client.describeCluster().nodes())
        .flatMapIterable(f -> KafkaFutures.join(f))
        .doOnNext(n -> log.debug("Found broker on host '{}'.", n.host()))
        .filter(n -> hostPredicate.test(n.host()))
        .next()
        .map(n -> new BrokerNode(n.id(), n.rack() == null ? "none" : n.rack(), n.host()))
        .doOnNext(b -> log.debug("Using broker {}.", b))
        .blockOptional()
        .orElseThrow(() -> new RuntimeException("No broker found on localhost!"));
  }

  @Data
  public static class BrokerNode {
    private final int id;
    private final @NonNull String rack;
    private final @NonNull String host;
  }
}
