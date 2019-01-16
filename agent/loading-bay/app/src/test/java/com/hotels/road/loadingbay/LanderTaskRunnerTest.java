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
package com.hotels.road.loadingbay;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.hive.metastore.HivePartitionManager;
import com.hotels.road.hive.metastore.MetaStoreException;
import com.hotels.road.loadingbay.LanderTaskRunner.State;
import com.hotels.road.loadingbay.event.HiveNotificationHandler;
import com.hotels.road.loadingbay.lander.Lander;
import com.hotels.road.loadingbay.lander.Lander.Factory;
import com.hotels.road.loadingbay.lander.LanderConfiguration;
import com.hotels.road.loadingbay.lander.OffsetRange;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@RunWith(MockitoJUnitRunner.class)
public class LanderTaskRunnerTest {

  private static final String LAST_RUN_PATH = "/destinations/hive/status/lastRun";
  private static final String ACQUISITION_INSTANT = "20180516T091704Z";
  private static final String PARTITION_SPEC = "acquisition_instant=" + ACQUISITION_INSTANT;
  private static final String S3_PREFIX = String.format("database1/road1/1526462225000/acquisition_instant=%s",
      ACQUISITION_INSTANT);
  private static final String TOPIC_NAME = "topic1";
  private static final String DATABASE = "database1";
  private static final String ROAD_NAME = "road1";

  private @Mock OffsetManager offsetManager;
  private @Mock HivePartitionManager hivePartitionManager;
  private @Mock Factory landerFactory;
  private @Mock HiveNotificationHandler landingNotifier;
  private @Mock PatchSetEmitter emitter;
  private @Mock Clock clock;
  private @Mock Lander lander;
  private @Mock Partition partition;

  private final long runtimeMillis = 1526462224000L;
  long maxRecordsPerPartition = 100L;
  private LanderTaskRunner underTest;
  private LanderConfiguration expectedLanderConfiguration;
  private String partitionColumnValue;
  private Map<Integer, OffsetRange> expectedOffsets;

  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final Supplier<Counter> metaStoreErrorsMeterSupplier = () -> Counter
      .builder("loading-bay.meta-store-errors")
      .tag("road", ROAD_NAME)
      .register(registry);
  private final Supplier<Counter> partitionMutationCounterSupplier = () -> Counter
      .builder("loading-bay.partition-mutations")
      .tag("road", ROAD_NAME)
      .register(registry);

  @Before
  public void setUp() {
    underTest = new LanderTaskRunner(registry, offsetManager, ROAD_NAME, TOPIC_NAME, DATABASE, hivePartitionManager,
        landerFactory, landingNotifier, emitter, clock, maxRecordsPerPartition, false);

    expectedOffsets = new HashMap<>();
    expectedOffsets.put(1, new OffsetRange(5, 10));
    expectedOffsets.put(2, new OffsetRange(15, 20));

    doReturn(1526462225000L).when(clock).millis();
    expectedLanderConfiguration = new LanderConfiguration(ROAD_NAME, TOPIC_NAME, expectedOffsets, S3_PREFIX, false,
        ACQUISITION_INSTANT, false);
  }

  @Test
  public void roadNameIsSet() {
    assertThat(underTest.getRoadName(), is(ROAD_NAME));
  }

  @Test
  public void startingStateIsIdle() {
    assertThat(underTest.getState(), is(LanderTaskRunner.State.IDLE));
  }

  @Test
  public void prepareLanderConfiguration() {
    Map<Integer, Long> latestOffsets = ImmutableMap.of(1, 10L, 2, 20L);
    Map<Integer, Long> committedOffsets = ImmutableMap.of(1, 5L, 2, 15L);

    when(offsetManager.getLatestOffsets(TOPIC_NAME)).thenReturn(latestOffsets);
    when(offsetManager.getCommittedOffsets(TOPIC_NAME)).thenReturn(committedOffsets);

    assertThat(underTest.getState(), is(State.IDLE));
    LanderConfiguration landerConfiguration = underTest.prepareLanderConfiguration(ACQUISITION_INSTANT);

    assertThat(landerConfiguration, is(expectedLanderConfiguration));
    assertThat(underTest.getState(), is(State.LANDING));
  }

  @Test(expected = NoDataToLandException.class)
  public void prepareLanderConfigurationOffsetsTheSame() {
    Map<Integer, Long> offsets = ImmutableMap.of(1, 10L, 2, 20L);

    when(offsetManager.getLatestOffsets(TOPIC_NAME)).thenReturn(offsets);
    when(offsetManager.getCommittedOffsets(TOPIC_NAME)).thenReturn(offsets);

    assertThat(underTest.getState(), is(State.IDLE));
    underTest.prepareLanderConfiguration(ACQUISITION_INSTANT);
  }

  @Test
  public void typicalUpdateMetadata() {
    underTest.changeState(State.LANDING);
    when(hivePartitionManager.addPartition(eq(ROAD_NAME), any(), any())).thenReturn(Optional.of(partition));
    underTest.updateMetadata(expectedLanderConfiguration);

    assertThat(underTest.getState(), is(State.UPDATING));

    verify(hivePartitionManager).addPartition(ROAD_NAME, singletonList(ACQUISITION_INSTANT), S3_PREFIX);
    verify(offsetManager).commitOffsets(TOPIC_NAME, ImmutableMap.of(1, 10L, 2, 20L));
    verify(landingNotifier).handlePartitionCreated(ROAD_NAME, partition, PARTITION_SPEC, 10L);
  }

  @Test
  public void metaStoreError() {
    underTest.changeState(State.LANDING);
    when(hivePartitionManager.addPartition(any(), any(), any()))
        .thenThrow(new MetaStoreException("meta-store-error", null));
    try {
      underTest.updateMetadata(expectedLanderConfiguration);
    } catch (Exception e) {
      assertThat(e, is(instanceOf(MetaStoreException.class)));
    }

    assertThat(metaStoreErrorsMeterSupplier.get().count(), is(1.0));
  }

  @Test
  public void partitionNotCreatedWhenUpdatingMetadata() {
    underTest.changeState(State.LANDING);
    when(hivePartitionManager.addPartition(eq(ROAD_NAME), any(), any())).thenReturn(Optional.empty());
    underTest.updateMetadata(expectedLanderConfiguration);

    assertThat(underTest.getState(), is(State.UPDATING));

    verify(hivePartitionManager).addPartition(ROAD_NAME, singletonList(ACQUISITION_INSTANT), S3_PREFIX);
    verify(offsetManager).commitOffsets(TOPIC_NAME, ImmutableMap.of(1, 10L, 2, 20L));

    assertThat(partitionMutationCounterSupplier.get().count(), is(1.0));
  }

  @Test
  public void runTypical() throws Exception {
    when(offsetManager.getLatestOffsets(TOPIC_NAME)).thenReturn(ImmutableMap.of(1, 10L, 2, 20L));
    when(offsetManager.getCommittedOffsets(TOPIC_NAME)).thenReturn(ImmutableMap.of(1, 5L, 2, 15L));

    when(landerFactory.newInstance(expectedLanderConfiguration)).thenReturn(lander);
    CompletableFuture<LanderConfiguration> future = new CompletableFuture<>();
    when(lander.run()).thenReturn(future);
    future.complete(expectedLanderConfiguration);
    when(hivePartitionManager.addPartition(eq(ROAD_NAME), any(), any())).thenReturn(Optional.of(partition));

    underTest.run(OffsetDateTime.ofInstant(Instant.ofEpochMilli(runtimeMillis), ZoneOffset.UTC));

    verify(offsetManager).getCommittedOffsets(TOPIC_NAME);
    verify(offsetManager).getLatestOffsets(TOPIC_NAME);
    verify(landerFactory).newInstance(expectedLanderConfiguration);
    verify(hivePartitionManager).addPartition(ROAD_NAME, singletonList(ACQUISITION_INSTANT), S3_PREFIX);
    verify(offsetManager).commitOffsets(TOPIC_NAME, ImmutableMap.of(1, 10L, 2, 20L));
    verify(landingNotifier).handlePartitionCreated(ROAD_NAME, partition, PARTITION_SPEC, 10L);
    verify(emitter)
        .emit(new PatchSet(ROAD_NAME, singletonList(PatchOperation.replace(LAST_RUN_PATH, "2018-05-16T09:17:04Z"))));
    verifyNoMoreInteractions(emitter);
  }

  @Test
  public void runChainTypical() throws Exception {
    underTest.runChain(partitionColumnValue);
  }

  @Test
  public void runChainNoDataToLandException() throws Exception {
    when(offsetManager.getCommittedOffsets(TOPIC_NAME)).thenReturn(singletonMap(0, 100L));
    when(offsetManager.getLatestOffsets(TOPIC_NAME)).thenReturn(singletonMap(0, 100L));
    underTest.runChain(partitionColumnValue);
    verify(emitter, never()).emit(any());
  }

  @Test
  public void onNoDataException() throws Exception {
    underTest.onException(ACQUISITION_INSTANT, new CompletionException(new NoDataToLandException()));
    verify(emitter, never()).emit(any());
  }

  @Test
  public void onOtherException() throws Exception {
    underTest.onException(ACQUISITION_INSTANT, new RuntimeException("test message"));
    verify(emitter, never()).emit(any());
  }
}
