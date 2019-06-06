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

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import static com.google.common.collect.Iterables.getOnlyElement;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.RequiredArgsConstructor;

import com.hotels.road.weighbridge.model.Broker;
import com.hotels.road.weighbridge.model.LogDir;
import com.hotels.road.weighbridge.model.PartitionReplica;
import com.hotels.road.weighbridge.model.Topic;

@RunWith(MockitoJUnitRunner.class)
public class WeighBridgeMetricsTest {
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

  private WeighBridgeMetrics underTest;

  @Before
  public void before() throws Exception {
    underTest = new WeighBridgeMetrics(registry);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void typical() throws Exception {
    PartitionReplica replica = new PartitionReplica(1, true, false, 3L, 2L, 4L, 5L, 1L);
    List<PartitionReplica> replicas = singletonList(replica);
    Topic topic = new Topic("topicName", Duration.ofMillis(2L), replicas);
    LogDir logDir = new LogDir("path", 1L, 3L, singletonList(topic));
    List<LogDir> logDirs = singletonList(logDir);
    Broker broker = new Broker(0, "rack", logDirs);

    underTest.update(broker);

    Matcher<Iterable<? extends Tag>> diskTags = containsInAnyOrder(tag("broker", "0"), tag("logdir", "path"),
        tag("rack", "rack"));
    Matcher<Iterable<? extends Tag>> replicaTags = containsInAnyOrder(tag("broker", "0"), tag("logdir", "path"),
        tag("rack", "rack"), tag("topic", "topicName"), tag("partition", "1"), tag("leader", "true"),
        tag("inSync", "false"));

    assertMeter(registry.get("weighbridge_disk_free").meter(), diskTags, 1.0);
    assertMeter(registry.get("weighbridge_disk_total").meter(), diskTags, 3.0);
    assertMeter(registry.get("weighbridge_disk_used").meter(), diskTags, 2.0);
    assertMeter(registry.get("weighbridge_size_on_disk").meter(), replicaTags, 3.0);
    assertMeter(registry.get("weighbridge_log_size").meter(), replicaTags, 2.0);
    assertMeter(registry.get("weighbridge_beginning_offset").meter(), replicaTags, 4.0);
    assertMeter(registry.get("weighbridge_end_offset").meter(), replicaTags, 5.0);
    assertMeter(registry.get("weighbridge_record_count").meter(), replicaTags, 1.0);
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
