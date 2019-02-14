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
package com.hotels.road.offramp.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import static com.hotels.road.offramp.metrics.StreamMetrics.BYTES;
import static com.hotels.road.offramp.metrics.StreamMetrics.COMMIT_FAILURE;
import static com.hotels.road.offramp.metrics.StreamMetrics.COMMIT_SUCCESS;
import static com.hotels.road.offramp.metrics.StreamMetrics.LATENCY;
import static com.hotels.road.offramp.metrics.StreamMetrics.MESSAGE;
import static com.hotels.road.offramp.metrics.StreamMetrics.OFFRAMP;
import static com.hotels.road.offramp.metrics.StreamMetrics.REBALANCE;

import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.google.common.collect.Iterables;


@RunWith(MockitoJUnitRunner.class)
public class OfframpMetricsTest {
  private static final String ROAD = "road";
  private static final String ROAD_NAME = "roadName";
  private static final String STREAM = "stream";
  private static final String STREAM_NAME = "streamName";
  private static final Tags ROAD_STREAM_TAGS = Tags.of(ROAD, ROAD_NAME).and(STREAM, STREAM_NAME);

  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final MeterPool pool = new MeterPool(registry);
  private @Mock Clock clock;
  private StreamMetrics underTest;

  @Test
  public void markCommit() throws Exception {
    Counter commitSuccessCounter = registry.counter(OFFRAMP + COMMIT_SUCCESS, ROAD_STREAM_TAGS);
    Counter commitFailureCounter = registry.counter(OFFRAMP + COMMIT_FAILURE, ROAD_STREAM_TAGS);

    underTest = new StreamMetrics.Factory(pool, clock).create(ROAD_NAME, STREAM_NAME);

    underTest.markCommit(true);
    assertThat(commitSuccessCounter.count(), is(1.0));

    underTest.markCommit(false);
    assertThat(commitFailureCounter.count(), is(1.0));
  }

  @Test
  public void markMessage() throws Exception {
    Counter messagesCounter = registry.counter(OFFRAMP + MESSAGE, ROAD_STREAM_TAGS);
    Counter bytesCounter = registry.counter(OFFRAMP + BYTES, ROAD_STREAM_TAGS);

    underTest = new StreamMetrics.Factory(pool, clock).create(ROAD_NAME, STREAM_NAME);
    underTest.markMessage(123L);

    assertThat(messagesCounter.count(), is(1.0));
    assertThat(bytesCounter.count(), is(123.0));
  }

  @Test
  public void markRebalance() throws Exception {
    Counter rebalanceCounter = registry.counter(OFFRAMP + REBALANCE, ROAD_STREAM_TAGS);

    underTest = new StreamMetrics.Factory(pool, clock).create(ROAD_NAME, STREAM_NAME);
    underTest.markRebalance();

    assertThat(rebalanceCounter.count(), is(1.0));
  }

  @Test
  public void markMessageLatency() throws Exception {
    doReturn(5L).when(clock).millis();

    underTest = new StreamMetrics.Factory(pool, clock).create(ROAD_NAME, STREAM_NAME);

    underTest.markMessageLatency(7, 3L);
    validateLatencyForPartition(7, 0.002); //latency should be 2 mills (5L (current time) - 3L (origin timestamp))

    //Update and check if gauge got updated
    underTest.markMessageLatency(7, 1L);
    validateLatencyForPartition(7, 0.004); //latency should be 4 mills (5L (current time) - 1L (origin timestamp))

    //Update for another partition and check if all gauges have correct values
    underTest.markMessageLatency(3, 2L);
    validateLatencyForPartition(3, 0.003); //latency should be 3 mills (5L (current time) - 2L (origin timestamp))
    validateLatencyForPartition(7, 0.004); //old one should remain the same.
  }

  private void validateLatencyForPartition(int partition, double expectedLatency) {
    List<Gauge> gauges = registry.getMeters()
            .stream()
            .filter(meter -> meter instanceof Gauge)
            .map(Gauge.class::cast)
            .filter(g -> Integer.toString(partition).equals(g.getId().getTag("partition")))
            .collect(Collectors.toList());

    assertThat(gauges, hasSize(1));
    Gauge gauge = gauges.get(0);
    assertThat(gauge.getId().getName(), is(OFFRAMP+LATENCY));
    assertThat(gauge.getId().getTags(), hasItems(Iterables.toArray(ROAD_STREAM_TAGS, Tag.class)));
    assertThat(gauge.value(), is(expectedLatency));
  }
}
