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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import com.hotels.road.truck.park.metrics.Metrics.SettableGauge;

@RunWith(MockitoJUnitRunner.class)
public class MetricsTest {

  @Mock
  private MetricRegistry metricRegistry;
  @Mock
  private Meter meter;

  @Test
  public void ctor() {
    new Metrics(metricRegistry);

    verify(metricRegistry).meter(Metrics.CONSUMED_BYTES);
    verify(metricRegistry).meter(Metrics.UPLOADED_BYTES);
    verify(metricRegistry).meter(Metrics.UPLOADED_EVENTS);
  }

  @Test
  public void consumedBytes() {
    when(metricRegistry.meter(Metrics.CONSUMED_BYTES)).thenReturn(meter);

    new Metrics(metricRegistry).consumedBytes(1L);

    verify(meter).mark(1L);
  }

  @Test
  public void uploadedBytes() {
    when(metricRegistry.meter(Metrics.UPLOADED_BYTES)).thenReturn(meter);

    new Metrics(metricRegistry).uploadedBytes(1L);

    verify(meter).mark(1L);
  }

  @Test
  public void uploadedEvents() {
    when(metricRegistry.meter(Metrics.UPLOADED_EVENTS)).thenReturn(meter);

    new Metrics(metricRegistry).uploadedEvents(1L);

    verify(meter).mark(1L);
  }

  @Test
  public void offsetHighwaterMark() {
    ArgumentCaptor<SettableGauge> gauge = ArgumentCaptor.forClass(SettableGauge.class);
    when(metricRegistry.register(eq("partition.0.highwater-mark"), gauge.capture())).thenReturn(null);

    new Metrics(metricRegistry).offsetHighwaterMark(0, 1L);

    verify(metricRegistry).register(eq("partition.0.highwater-mark"), gauge.capture());
    assertThat(gauge.getValue().getValue(), is(1L));
  }

}
