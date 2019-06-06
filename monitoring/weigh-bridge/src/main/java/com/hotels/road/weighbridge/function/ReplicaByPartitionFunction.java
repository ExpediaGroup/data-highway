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
package com.hotels.road.weighbridge.function;

import static java.util.Collections.singleton;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Component
@RequiredArgsConstructor
public class ReplicaByPartitionFunction {
  private final AdminClient client;

  public Map<TopicPartition, Replica> apply(int brokerId) {
    return Mono
        .fromSupplier(() -> client.describeLogDirs(singleton(brokerId)).values().get(brokerId))
        .map(KafkaFutures::join)
        .flatMapIterable(Map::entrySet)
        .flatMap(e -> Flux.fromIterable(e.getValue().replicaInfos.entrySet()).map(
            entry -> Tuples.of(entry.getKey(), new Replica(e.getKey(), entry.getValue().size))))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .block();
  }

  @Data
  public static class Replica {
    private final String logDir;
    private final long size;
  }
}
