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

import static java.util.Collections.emptySortedMap;
import static java.util.stream.Collectors.toMap;

import static scala.collection.JavaConversions.asJavaCollection;
import static scala.collection.JavaConversions.mapAsJavaMap;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.common.collect.Maps;

@Component
@RequiredArgsConstructor
@Slf4j
class KafkaOffsetMetrics {
  private final AdminClient adminClient;
  private final ScheduledReporter reporter;

  @Scheduled(initialDelayString = "${metricRate:60000}", fixedRateString = "${metricRate:60000}")
  void sendOffsets() {
    log.info("Sending offsets");
    // We're calling listAllGroupsFlattened as opposed to listAllConsumerGroupsFlattened because we're not
    // consuming data with the KafkaConsumer in hive-agent (it doesn't subscribe/poll), so when it commits the
    // protocolType is null when it would otherwise be 'consumer'.
    @SuppressWarnings("rawtypes")
    SortedMap<String, Gauge> gauges = asJavaCollection(adminClient.listAllGroupsFlattened())
        .stream()
        .map(GroupOverview::groupId)
        .flatMap(groupId -> {
          log.info("Found groupId: {}", groupId);
          return mapAsJavaMap(adminClient.listGroupOffsets(groupId)).entrySet().stream().map(
              entry -> createMetric(groupId, entry));
        })
        .collect(toMap(Entry::getKey, Entry::getValue, (a, b) -> a, () -> new TreeMap<>()));

    reporter.report(gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap());
    log.info("Done");
  }

  @SuppressWarnings("rawtypes")
  private Entry<String, Gauge> createMetric(String groupId, Entry<TopicPartition, Object> entry) {
    return Maps.immutableEntry(metricName(groupId, entry.getKey()), (Gauge) () -> entry.getValue());
  }

  private String metricName(String groupId, TopicPartition topicPartition) {
    return MetricRegistry.name("group", clean(groupId), "topic", clean(topicPartition.topic()), "partition",
        Integer.toString(topicPartition.partition()), "offset");
  }

  private String clean(String name) {
    return name.replaceAll("\\.", "_");
  }

}
