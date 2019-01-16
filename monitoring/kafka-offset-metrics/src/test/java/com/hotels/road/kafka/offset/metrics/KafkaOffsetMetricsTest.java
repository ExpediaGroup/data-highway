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
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static scala.collection.JavaConversions.asScalaSet;
import static scala.collection.JavaConversions.mapAsScalaMap;

import java.util.Map;
import java.util.SortedMap;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import scala.Predef;
import scala.Tuple2;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.ScheduledReporter;

@RunWith(MockitoJUnitRunner.class)
public class KafkaOffsetMetricsTest {

  @Mock
  private AdminClient adminClient;
  @Mock
  private ScheduledReporter reporter;

  private KafkaOffsetMetrics underTest;

  @Before
  public void before() {
    underTest = new KafkaOffsetMetrics(adminClient, reporter);
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

    underTest.sendOffsets();

    ArgumentCaptor<SortedMap<String, Gauge>> gaugesCaptor = ArgumentCaptor.forClass(SortedMap.class);

    verify(reporter).report(gaugesCaptor.capture(), eq(emptySortedMap()), eq(emptySortedMap()), eq(emptySortedMap()),
        eq(emptySortedMap()));

    SortedMap<String, Gauge> gauges = gaugesCaptor.getValue();
    assertThat(gauges.size(), is(1));
    Gauge gauge = gauges.get("group.groupId.topic.topicName.partition.0.offset");
    assertThat(gauge.getValue(), is(1L));
  }

  private scala.collection.immutable.Map<TopicPartition, Object> asScalaMap(Map<TopicPartition, Object> map) {
    return mapAsScalaMap(map).toMap(Predef.<Tuple2<TopicPartition, Object>> conforms());
  }

}
