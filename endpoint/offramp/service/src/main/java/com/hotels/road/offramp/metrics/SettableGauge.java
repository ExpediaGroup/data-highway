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

import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class SettableGauge implements Gauge {
  private final Gauge gauge;
  private final AtomicLong value;

  public void setValue(long newValue) {
    value.set(newValue);
  }

  public void increment() {
    value.incrementAndGet();
  }

  public void decrement() {
    value.decrementAndGet();
  }

  @Override
  public double value() {
    return gauge.value();
  }

  @Override
  public Id getId() {
    return gauge.getId();
  }
}
