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

import static com.google.common.base.Charsets.UTF_8;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OnrampMetrics {
  private static final String ROAD = "road";

  private final MeterRegistry registry;

  public void markSuccessMetrics(String roadName, String message) {
    markSuccessMetrics(roadName, message.getBytes(UTF_8).length);
  }

  public void markSuccessMetrics(String roadName, long avroBytes) {
    Iterable<Tag> tags = singleton(Tag.of(ROAD, roadName));
    registry.counter("onramp.send-success", tags).increment();
    DistributionSummary
        .builder("onramp.message-size")
        .tags(tags)
        .publishPercentileHistogram()
        .register(registry)
        .record(avroBytes);
  }

  public void markValidationFailures(String roadName) {
    registry.counter("onramp.validation-failures", singleton(Tag.of(ROAD, roadName))).increment();
  }

  public void markFailureMetrics(String roadName) {
    registry.counter("onramp.send-failures", singleton(Tag.of(ROAD, roadName))).increment();
  }
}
