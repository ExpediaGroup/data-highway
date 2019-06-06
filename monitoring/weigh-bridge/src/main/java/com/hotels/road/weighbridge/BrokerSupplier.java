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
package com.hotels.road.weighbridge;

import static java.time.Duration.ZERO;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toSet;

import static com.google.common.collect.Lists.newArrayList;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import com.hotels.road.weighbridge.function.BrokerNodeFunction;
import com.hotels.road.weighbridge.function.BrokerNodeFunction.BrokerNode;
import com.hotels.road.weighbridge.function.DiskByLogDirFunction;
import com.hotels.road.weighbridge.function.DiskByLogDirFunction.Disk;
import com.hotels.road.weighbridge.function.LeaderInSyncByPartitionFunction;
import com.hotels.road.weighbridge.function.LeaderInSyncByPartitionFunction.LeaderInSync;
import com.hotels.road.weighbridge.function.OffsetsByPartitionFunction;
import com.hotels.road.weighbridge.function.OffsetsByPartitionFunction.Offsets;
import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction;
import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction.Replica;
import com.hotels.road.weighbridge.function.RetentionByTopicFunction;
import com.hotels.road.weighbridge.function.SizeByPartitionFunction;
import com.hotels.road.weighbridge.model.Broker;
import com.hotels.road.weighbridge.model.LogDir;
import com.hotels.road.weighbridge.model.PartitionReplica;
import com.hotels.road.weighbridge.model.Topic;

@Component
@RequiredArgsConstructor
public class BrokerSupplier implements Supplier<Broker> {
  private final Predicate<String> hostPredicate;
  private final BrokerNodeFunction brokerNodeFunction;
  private final ReplicaByPartitionFunction replicaByPartitionFunction;
  private final LeaderInSyncByPartitionFunction leaderInSyncByPartitionFunction;
  private final RetentionByTopicFunction retentionByTopicFunction;
  private final OffsetsByPartitionFunction offsetsByPartitionFunction;
  private final SizeByPartitionFunction sizeByPartitionFunction;
  private final DiskByLogDirFunction diskByLogDirFunction;

  @Override
  public Broker get() {
    BrokerNode brokerNode = brokerNodeFunction.apply(hostPredicate);
    Map<TopicPartition, Replica> replicaByPartition = replicaByPartitionFunction.apply(brokerNode.getId());

    Set<TopicPartition> partitions = replicaByPartition.keySet();
    Set<String> topics = Flux.fromIterable(partitions).map(TopicPartition::topic).collect(toSet()).block();

    Map<TopicPartition, LeaderInSync> leaderInSyncByPartition = leaderInSyncByPartitionFunction
        .apply(brokerNode.getId(), topics);
    Map<String, Duration> retentionByTopic = retentionByTopicFunction.apply(topics);
    Map<TopicPartition, Offsets> offsetsByPartition = offsetsByPartitionFunction.apply(partitions);
    Map<TopicPartition, Long> sizeByPartition = sizeByPartitionFunction.apply(replicaByPartition);
    Map<String, Disk> diskByLogDir = diskByLogDirFunction.apply(replicaByPartition);

    return Flux
        .fromIterable(partitions)
        .map(p -> replica(replicaByPartition, offsetsByPartition, leaderInSyncByPartition, sizeByPartition, p))
        .groupBy(Tuple3::getT1)
        .flatMap(byLogDir -> byLogDir
            .sort(comparing(Tuple3::getT2))
            .windowUntil(keyChanged(Tuple3::getT2), true)
            .flatMap(byTopic -> byTopic
                .collectMultimap(Tuple3::getT2, Tuple3::getT3)
                .flatMapIterable(Map::entrySet)
                .map(e -> new Topic(e.getKey(), retentionByTopic.getOrDefault(e.getKey(), ZERO),
                    newArrayList(e.getValue()))))
            .collectList()
            .map(ts -> logDir(diskByLogDir, byLogDir, ts)))
        .collectList()
        .map(lds -> new Broker(brokerNode.getId(), brokerNode.getRack(), lds))
        .block();
  }

  private Tuple3<String, String, PartitionReplica> replica(
      Map<TopicPartition, Replica> replicaByPartition,
      Map<TopicPartition, Offsets> offsetsByPartition,
      Map<TopicPartition, LeaderInSync> leaderInSyncByPartition,
      Map<TopicPartition, Long> sizeByPartition,
      TopicPartition p) {
    LeaderInSync leaderInSync = leaderInSyncByPartition.getOrDefault(p, new LeaderInSync(false, false));
    Offsets offsets = offsetsByPartition.getOrDefault(p, new Offsets(0L, 0L));
    Replica replica = replicaByPartition.getOrDefault(p, new Replica("", 0L));
    PartitionReplica partitionReplica = new PartitionReplica(p.partition(), leaderInSync.isLeader(),
        leaderInSync.isInSync(), sizeByPartition.getOrDefault(p, 0L), replica.getSize(), offsets.getBeginning(),
        offsets.getEnd(), offsets.getCount());
    return Tuples.of(replica.getLogDir(), p.topic(), partitionReplica);
  }

  private LogDir logDir(
      Map<String, Disk> diskByLogDir,
      GroupedFlux<String, Tuple3<String, String, PartitionReplica>> byLogDir,
      List<Topic> ts) {
    Disk disk = diskByLogDir.getOrDefault(byLogDir.key(), new Disk(0L, 0L));
    return new LogDir(byLogDir.key(), disk.getFree(), disk.getTotal(), ts);
  }

  private static <T, K> Predicate<T> keyChanged(Function<T, K> keyFunction) {
    AtomicReference<K> key = new AtomicReference<>();
    return t -> {
      K k = keyFunction.apply(t);
      K prev = key.get();
      boolean cut = false;
      if (prev == null || !prev.equals(k)) {
        cut = true;
      }
      key.set(k);
      return cut;
    };
  }
}
