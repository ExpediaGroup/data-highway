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
package com.hotels.road.onramp.kafka;

import static java.util.Collections.singleton;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@RunWith(MockitoJUnitRunner.class)
public class OnrampMetricsTest {
  private static final String ROAD = "road";
  private static final String ROAD_NAME = "roadName";

  private static final Iterable<Tag> tags = singleton(Tag.of(ROAD, ROAD_NAME));

  private MeterRegistry registry = new SimpleMeterRegistry();
  private Supplier<Counter> sendSuccess = () -> registry.counter("onramp.send-success", tags);
  private Supplier<DistributionSummary> messageSize = () -> registry.summary("onramp.message-size", tags);
  private Supplier<Counter> validationFailures = () -> registry.counter("onramp.validation-failures", tags);
  private Supplier<Counter> sendFailures = () -> registry.counter("onramp.send-failures", tags);

  private OnrampMetrics underTest = new OnrampMetrics(registry);;

  @Test
  public void markSuccessMetricsString() {
    String message = "message";
    long size = message.getBytes().length;

    underTest.markSuccessMetrics(ROAD_NAME, message);

    assertThat(sendSuccess.get().count(), is(1.0));
    assertThat(messageSize.get().takeSnapshot().total(), is((double) size));
  }

  @Test
  public void markSuccessMetricsLong() {
    long size = 7;

    underTest.markSuccessMetrics(ROAD_NAME, size);

    assertThat(sendSuccess.get().count(), is(1.0));
    assertThat(messageSize.get().takeSnapshot().total(), is((double) size));
  }

  @Test
  public void markValidationFailures() {
    underTest.markValidationFailures(ROAD_NAME);

    assertThat(validationFailures.get().count(), is(1.0));
  }

  @Test
  public void markFailureMetrics() {
    underTest.markFailureMetrics(ROAD_NAME);

    assertThat(sendFailures.get().count(), is(1.0));
  }
}
