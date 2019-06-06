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
package com.hotels.road.truck.park.metrics;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

@Component
public class Metrics {

  static final String CONSUMED_BYTES = "consumed-bytes";
  static final String UPLOADED_BYTES = "uploaded-bytes";
  static final String UPLOADED_EVENTS = "uploaded-events";
  static final String HIGHWATER_MARK = "highwater-mark";

  private final MetricRegistry registry;
  private final Meter consumedBytes;
  private final Meter uploadedBytes;
  private final Meter uploadedEvents;
  private final Map<Integer, SettableGauge> highwaterMarks = new HashMap<>();

  @Autowired
  Metrics(MetricRegistry registry) {
    this.registry = registry;
    consumedBytes = registry.meter(CONSUMED_BYTES);
    uploadedBytes = registry.meter(UPLOADED_BYTES);
    uploadedEvents = registry.meter(UPLOADED_EVENTS);
  }

  public void consumedBytes(long byteCount) {
    consumedBytes.mark(byteCount);
  }

  public void uploadedBytes(long byteCount) {
    uploadedBytes.mark(byteCount);
  }

  public void uploadedEvents(long eventCount) {
    uploadedEvents.mark(eventCount);
  }

  public void offsetHighwaterMark(int partition, long offset) {
    highwaterMarks.computeIfAbsent(partition, this::createGauge).set(offset);
  }

  private SettableGauge createGauge(int partition) {
    String name = MetricRegistry.name("partition", Integer.toString(partition), HIGHWATER_MARK);
    SettableGauge gauge = new SettableGauge();
    registry.register(name, gauge);
    return gauge;
  }

  static class SettableGauge implements Gauge<Long> {

    private long value;

    void set(long value) {
      this.value = value;
    }

    @Override
    public Long getValue() {
      return value;
    }

  }

}
