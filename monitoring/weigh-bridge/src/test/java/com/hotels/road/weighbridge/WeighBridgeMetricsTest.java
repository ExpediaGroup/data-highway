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
package com.hotels.road.weighbridge;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import static io.prometheus.client.Collector.Type.GAUGE;

import static com.hotels.road.weighbridge.WeighBridgeMetrics.LOGDIR_LABELS;
import static com.hotels.road.weighbridge.WeighBridgeMetrics.REPLICA_LABELS;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.hotels.road.weighbridge.model.Broker;
import com.hotels.road.weighbridge.model.LogDir;
import com.hotels.road.weighbridge.model.PartitionReplica;
import com.hotels.road.weighbridge.model.Topic;

@RunWith(MockitoJUnitRunner.class)
public class WeighBridgeMetricsTest {
  private @Mock Supplier<Broker> supplier;

  private WeighBridgeMetrics underTest;

  @Before
  public void before() throws Exception {
    underTest = new WeighBridgeMetrics(supplier);
  }

  @Test
  public void typical() throws Exception {
    PartitionReplica replica = new PartitionReplica(1, true, false, 3L, 2L, 4L, 5L, 1L);
    List<PartitionReplica> replicas = singletonList(replica);
    Topic topic = new Topic("topicName", Duration.ofMillis(2L), replicas);
    LogDir logDir = new LogDir("path", 1L, 3L, singletonList(topic));
    List<LogDir> logDirs = singletonList(logDir);
    Broker broker = new Broker(0, "rack", logDirs);

    doReturn(broker).when(supplier).get();

    List<MetricFamilySamples> result = underTest.collect();
    assertThat(result.size(), is(1));
    MetricFamilySamples metricFamilySamples = result.get(0);
    assertThat(metricFamilySamples.name, is("weighbridge"));
    assertThat(metricFamilySamples.help, is("weighbridge"));
    assertThat(metricFamilySamples.type, is(GAUGE));
    List<Sample> samples = metricFamilySamples.samples;
    assertThat(samples.size(), is(8));

    ImmutableMap<String, Sample> samplesMap = Maps.uniqueIndex(samples, s -> s.name);

    ImmutableList<String> labelValues;
    labelValues = ImmutableList.of("0", "path");

    Sample sample;

    sample = samplesMap.get("weighbridge_disk_free");
    assertThat(sample.labelNames, is(LOGDIR_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(1.0));

    sample = samplesMap.get("weighbridge_disk_total");
    assertThat(sample.labelNames, is(LOGDIR_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(3.0));

    sample = samplesMap.get("weighbridge_disk_used");
    assertThat(sample.labelNames, is(LOGDIR_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(2.0));

    labelValues = ImmutableList.of("0", "path", "topicName", "1", "true", "false");

    sample = samplesMap.get("weighbridge_size_on_disk");
    assertThat(sample.labelNames, is(REPLICA_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(3.0));

    sample = samplesMap.get("weighbridge_log_size");
    assertThat(sample.labelNames, is(REPLICA_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(2.0));

    sample = samplesMap.get("weighbridge_beginning_offset");
    assertThat(sample.labelNames, is(REPLICA_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(4.0));

    sample = samplesMap.get("weighbridge_end_offset");
    assertThat(sample.labelNames, is(REPLICA_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(5.0));

    sample = samplesMap.get("weighbridge_record_count");
    assertThat(sample.labelNames, is(REPLICA_LABELS));
    assertThat(sample.labelValues, is(labelValues));
    assertThat(sample.value, is(1.0));
  }
}
