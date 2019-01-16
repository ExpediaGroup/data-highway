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
package com.hotels.road.highwaypatrol;

import static java.util.Collections.singleton;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MessagesMetricSetTest {
  private MeterRegistry registry;
  private MessagesMetricSet metrics;

  @Before
  public void before() throws Exception {
    registry = new SimpleMeterRegistry();
    metrics = new MessagesMetricSet(registry);
  }

  @Test
  public void correct_metrics_registered() throws Exception {
    List<Meter> metrics = registry.getMeters();
    assertTrue(containsAtLeastOne(metrics, m -> "highwaypatrol-receiverErrors".equals(m.getId().getName())));
    assertTrue(containsAtLeastOne(metrics, m -> "highwaypatrol-onrampTime".equals(m.getId().getName())));
    assertTrue(containsAtLeastOne(metrics, m -> "highwaypatrol-transitTime".equals(m.getId().getName())));
    assertTrue(containsAtLeastOne(metrics, m -> "highwaypatrol-messagesCounted".equals(m.getId().getName())));
    for (MessageState state : MessageState.values()) {
      assertTrue(containsAtLeastOne(metrics, m -> "highwaypatrol-endState".equals(m.getId().getName())
          && state.getMetricName().equals(m.getId().getTag("state"))));
    }
  }

  <T> boolean containsAtLeastOne(Iterable<T> iterable, Predicate<T> predicate) {
    for (T item : iterable) {
      if (predicate.test(item)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void end_state_is_logged_and_accumulated() throws Exception {
    Map<MessageState, AtomicInteger> stateCounts = new EnumMap<>(MessageState.class);
    for (MessageState state : MessageState.values()) {
      stateCounts.put(state, new AtomicInteger());
    }
    long count = 0;
    Random r = new Random();
    while (count < 1000) {
      MessageState activeState = MessageState.values()[r.nextInt(MessageState.values().length)];
      metrics.markMessageEndState(activeState);
      count++;
      stateCounts.get(activeState).getAndIncrement();

      for (MessageState checkingState : MessageState.values()) {
        Counter metric = registry.counter("highwaypatrol-endState",
            singleton(Tag.of("state", checkingState.getMetricName())));
        assertThat(metric.count(), is((double) stateCounts.get(checkingState).get()));
      }

      assertThat(registry.counter("highwaypatrol-messagesCounted").count(), is((double) count));
    }
  }
}
