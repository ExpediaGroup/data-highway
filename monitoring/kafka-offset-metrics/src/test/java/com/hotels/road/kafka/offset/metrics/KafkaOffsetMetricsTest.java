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
package com.hotels.road.kafka.offset.metrics;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static scala.collection.JavaConversions.asScalaSet;
import static scala.collection.JavaConversions.mapAsScalaMap;

import static com.google.common.collect.Iterables.getOnlyElement;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import lombok.RequiredArgsConstructor;
import scala.Predef;
import scala.Tuple2;

@RunWith(MockitoJUnitRunner.class)
public class KafkaOffsetMetricsTest {

  private @Mock AdminClient adminClient;

  private final MeterRegistry registry = new SimpleMeterRegistry();

  private KafkaOffsetMetrics underTest;

  @Before
  public void before() {
    underTest = new KafkaOffsetMetrics(adminClient, registry);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void happyPath() {
    scala.collection.immutable.List<GroupOverview> listAllGroupsFlattened = asScalaSet(
        singleton(GroupOverview.apply("groupId", ""))).toList();
    when(adminClient.listAllGroupsFlattened()).thenReturn(listAllGroupsFlattened);

    scala.collection.immutable.Map<TopicPartition, Object> offsets = asScalaMap(
        singletonMap(new TopicPartition("topicName", 0), (Object) 1L));
    when(adminClient.listGroupOffsets("groupId")).thenReturn(offsets);

    underTest.refresh();

    Matcher<Iterable<? extends Tag>> tags = containsInAnyOrder(tag("group", "groupId"), tag("topic", "topicName"),
        tag("partition", "0"));

    assertMeter(registry.get("kafka_offset").meter(), tags, 1);
  }

  private scala.collection.immutable.Map<TopicPartition, Object> asScalaMap(Map<TopicPartition, Object> map) {
    return mapAsScalaMap(map).toMap(Predef.<Tuple2<TopicPartition, Object>> conforms());
  }

  void assertMeter(Meter meter, Matcher<Iterable<? extends Tag>> tags, double value) {
    assertThat(meter.getId().getTags(), tags);
    assertThat(getOnlyElement(meter.measure()).getValue(), is(value));
  }

  static Matcher<Tag> tag(String key, String value) {
    return new TagMatcher(Tag.of(key, value));
  }

  @RequiredArgsConstructor
  static class TagMatcher extends BaseMatcher<Tag> {
    private final Tag expected;

    @Override
    public boolean matches(Object item) {
      Tag actual = (Tag) item;
      return Objects.equals(expected.getKey(), actual.getKey())
          && Objects.equals(expected.getValue(), actual.getValue());
    }

    @Override
    public void describeTo(Description description) {}
  }
}
