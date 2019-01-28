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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaOffsetMetrics extends Collector {
  private final AdminClient adminClient;
  private final Supplier<String> hostnameSupplier;
  private final CollectorRegistry collectorRegistry;

  private static final List<String> LABELS =  Arrays.asList("host", "group", "topic", "partition");

  @PostConstruct
  public void registerExporter() {
    collectorRegistry.register(this);
  }

  @Override
  public List<MetricFamilySamples> collect() {

    List<Sample> samples = asJavaCollection(adminClient.listAllGroupsFlattened())
        .stream()
        .map(GroupOverview::groupId)
        .flatMap(groupId ->
        {
          log.info("Found groupId: {}", groupId);
          return mapAsJavaMap(adminClient.listGroupOffsets(groupId))
              .entrySet()
              .stream()
              .map(entry -> createSample(groupId, entry));
        })
        .collect(Collectors.toList());

    MetricFamilySamples mfs = new MetricFamilySamples("kafka-offset", Type.GAUGE, "kafka-offset", samples);

    return Collections.singletonList(mfs);
  }

  private Sample createSample(String groupId, Map.Entry<TopicPartition, Object> entry) {
    String partition = Integer.toString(entry.getKey().partition());
    String topic = clean(entry.getKey().topic());

    List<String> labelValues = Arrays.asList(hostnameSupplier.get(), clean(groupId), topic, partition);
    Number offset = (Number) entry.getValue();

    return new Sample("kafka-offset", LABELS, labelValues, offset.doubleValue());
  }

  private String clean(String name) {
    return name.replaceAll("\\.", "_");
  }
}
