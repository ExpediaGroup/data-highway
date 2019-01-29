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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import static io.prometheus.client.Collector.Type.GAUGE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static scala.collection.JavaConversions.asScalaSet;
import static scala.collection.JavaConversions.mapAsScalaMap;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import scala.Predef;
import scala.Tuple2;

@RunWith(MockitoJUnitRunner.class)
public class KafkaOffsetMetricsTest {

  @Mock
  private AdminClient adminClient;

  @Mock
  private Supplier<String> hostnameSupplier;

  @Mock
  private CollectorRegistry collectorRegistry;

  private KafkaOffsetMetrics underTest;

  @Before
  public void before() {
    underTest = new KafkaOffsetMetrics(adminClient, hostnameSupplier, collectorRegistry);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void happyPath() {
    scala.collection.immutable.List<GroupOverview> listAllGroupsFlattened = asScalaSet(
        singleton(GroupOverview.apply("groupId", ""))).toList();
    when(adminClient.listAllGroupsFlattened()).thenReturn(listAllGroupsFlattened);

    scala.collection.immutable.Map<TopicPartition, Object> offsets = asScalaMap(
        singletonMap(new TopicPartition("topicName", 0), (Object) 1L));
    when(adminClient.listGroupOffsets("groupId")).thenReturn(offsets);

    when(hostnameSupplier.get()).thenReturn("localhost");

    List<Collector.MetricFamilySamples> collection = underTest.collect();

    assertThat(collection.size(), is(1));
    Collector.MetricFamilySamples mfs = collection.get(0);

    assertThat(mfs.name, is("kafka-offset"));
    assertThat(mfs.type, is(GAUGE));
    assertThat(mfs.samples, is(not(empty())));
    assertThat(mfs.samples, is(hasSize(1)));

    Collector.MetricFamilySamples.Sample sample = mfs.samples.get(0);

    assertThat(sample.name, is("kafka-offset"));
    assertThat(sample.labelNames, is(asList("host", "group", "topic", "partition")));
    assertThat(sample.labelValues, is(asList("localhost", "groupId", "topicName", "0")));
    assertThat(sample.value, is(1.0d));
  }

  private scala.collection.immutable.Map<TopicPartition, Object> asScalaMap(Map<TopicPartition, Object> map) {
    return mapAsScalaMap(map).toMap(Predef.<Tuple2<TopicPartition, Object>> conforms());
  }
}
