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
package com.hotels.road.security;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public class SecurityMetrics {
  private final MeterRegistry registry;
  private final @Getter String counterName;

  public void increment(String roadName, AuthenticationType type, AuthorisationOutcome outcome) {
    ImmutableList<Tag> tags = ImmutableList
        .<Tag> builder()
        .add(Tag.of("road", roadName))
        .add(Tag.of("authentication", type.name()))
        .add(Tag.of("authorisation", outcome.name()))
        .build();
    registry.counter(counterName, tags).increment();
  }
}
