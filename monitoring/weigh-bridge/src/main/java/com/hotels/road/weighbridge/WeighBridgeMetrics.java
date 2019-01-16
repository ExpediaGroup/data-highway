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
package com.hotels.road.weighbridge;

import static io.prometheus.client.Collector.Type.GAUGE;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.google.common.collect.ImmutableList;

import com.hotels.road.weighbridge.model.Broker;
import com.hotels.road.weighbridge.model.LogDir;
import com.hotels.road.weighbridge.model.PartitionReplica;
import com.hotels.road.weighbridge.model.Topic;

@Component
@RequiredArgsConstructor
public class WeighBridgeMetrics extends Collector {
  static final List<String> LOGDIR_LABELS = ImmutableList.of("broker", "logdir");
  static final List<String> REPLICA_LABELS = ImmutableList.of("broker", "logdir", "topic", "partition", "leader",
      "inSync");

  private final Supplier<Broker> supplier;

  @Override
  public List<MetricFamilySamples> collect() {
    return Mono
        .fromSupplier(supplier)
        .flatMapMany(b -> Flux//
            .fromIterable(b.getLogDirs())
            .flatMap(ld -> logDir(b, ld)//
                .mergeWith(Flux//
                    .fromIterable(ld.getTopics())
                    .flatMap(t -> Flux//
                        .fromIterable(t.getPartitionReplicas())
                        .flatMap(p -> partitionReplica(b, ld, t, p))))))
        .collectList()
        .map(samples -> new MetricFamilySamples("weighbridge", GAUGE, "weighbridge", samples))
        .flux()
        .collectList()
        .block();
  }

  Flux<Sample> logDir(Broker b, LogDir ld) {
    List<String> labelValues = ImmutableList.of(String.valueOf(b.getId()), ld.getPath());
    Sample free = sample("weighbridge_disk_free", LOGDIR_LABELS, labelValues, ld.getDiskFree());
    Sample total = sample("weighbridge_disk_total", LOGDIR_LABELS, labelValues, ld.getDiskTotal());
    Sample used = sample("weighbridge_disk_used", LOGDIR_LABELS, labelValues, ld.getDiskTotal() - ld.getDiskFree());
    return Flux.just(free, total, used);
  }

  Flux<Sample> partitionReplica(Broker b, LogDir ld, Topic t, PartitionReplica p) {
    List<String> labelValues = Flux
        .just(b.getId(), ld.getPath(), t.getName(), p.getPartition(), p.isLeader(), p.isInSync())
        .map(Objects::toString)
        .collectList()
        .block();

    Sample sizeOnDisk = sample("weighbridge_size_on_disk", REPLICA_LABELS, labelValues, p.getSizeOnDisk());
    Sample logSize = sample("weighbridge_log_size", REPLICA_LABELS, labelValues, p.getLogSize());
    Sample beginning = sample("weighbridge_beginning_offset", REPLICA_LABELS, labelValues, p.getBeginningOffset());
    Sample end = sample("weighbridge_end_offset", REPLICA_LABELS, labelValues, p.getEndOffset());
    Sample count = sample("weighbridge_record_count", REPLICA_LABELS, labelValues, p.getRecordCount());
    return Flux.just(sizeOnDisk, logSize, beginning, end, count);
  }

  private Sample sample(String name, List<String> labelNames, List<String> labelValues, Long value) {
    return new Sample(name, labelNames, labelValues, value.doubleValue());
  }
}
