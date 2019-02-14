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
package com.hotels.road.kafka.offset.metrics;

import static scala.collection.JavaConversions.asJavaCollection;
import static scala.collection.JavaConversions.mapAsJavaMap;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tags;
import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class KafkaOffsetMetrics {
  private final AdminClient adminClient;
  private final MultiGauge gauge;

  public KafkaOffsetMetrics(AdminClient adminClient, MeterRegistry registry) {
    this.adminClient = adminClient;
    gauge = MultiGauge.builder("kafka_offset").register(registry);
  }

  @Scheduled(fixedDelayString = "${refreshPeriod:PT10S}")
  public void refresh() {
    Flux
        .fromIterable(asJavaCollection(adminClient.listAllGroupsFlattened()))
        .map(GroupOverview::groupId)
        .doOnNext(groupId -> log.info("Found groupId: {}", groupId))
        .flatMap(groupId -> Mono
            .just(mapAsJavaMap(adminClient.listGroupOffsets(groupId)))
            .flatMapIterable(Map::entrySet)
            .map(entry -> toRow(groupId, entry)))
        .collectList()
        .doOnNext(rows -> gauge.register(rows, true))
        .then()
        .block();
  }

  private Row toRow(String groupId, Entry<TopicPartition, Object> entry) {
    Tags tags = Tags.of("group", groupId)
        .and("topic", entry.getKey().topic())
        .and("partition", String.valueOf(entry.getKey().partition()));
    return Row.of(tags, (Number) entry.getValue());
  }
}
