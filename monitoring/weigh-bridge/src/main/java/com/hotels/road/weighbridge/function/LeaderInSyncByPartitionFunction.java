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

import java.util.Collection;
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
public class LeaderInSyncByPartitionFunction {
  private final AdminClient client;

  public Map<TopicPartition, LeaderInSync> apply(int brokerId, Collection<String> topics) {
    return Mono
        .fromSupplier(() -> client.describeTopics(topics).all())
        .map(KafkaFutures::join)
        .flatMapIterable(Map::values)
        .flatMap(td -> Flux
            .fromIterable(td.partitions())
            .filter(tpi -> Flux.fromIterable(tpi.replicas()).any(n -> n.id() == brokerId).block())
            .map(tpi -> {
              TopicPartition partition = new TopicPartition(td.name(), tpi.partition());
              boolean leader = tpi.leader() == null ? false : tpi.leader().id() == brokerId;
              boolean inSync = Flux.fromIterable(tpi.isr()).any(n -> n.id() == brokerId).block();
              return Tuples.of(partition, new LeaderInSync(leader, inSync));
            }))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .block();
  }

  @Data
  public static class LeaderInSync {
    private final boolean leader;
    private final boolean inSync;
  }
}
