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
package com.hotels.road.loadingbay;

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LanderMonitorTest {

  private static final Instant FIVE_MINUTES_PAST_EPOCH = Instant.ofEpochMilli(MINUTES.toMillis(5));
  private static final Instant THREE_MINUTES_PAST_EPOCH = Instant.ofEpochMilli(MINUTES.toMillis(3));
  private static final Instant TEN_MINUTES_PAST_EPOCH = Instant.ofEpochMilli(MINUTES.toMillis(10));
  private static final Instant EPOCH = Instant.ofEpochMilli(MINUTES.toMillis(0));
  private static final String PT1M = Duration.ofMinutes(1).toString();
  private static final String PT15M = Duration.ofMinutes(15).toString();

  private static final String ROAD_NAME = "road1";

  private static Clock clock = Clock.fixed(TEN_MINUTES_PAST_EPOCH, ZoneOffset.UTC);

  private static final OffsetDateTime RUNTIME = OffsetDateTime.ofInstant(TEN_MINUTES_PAST_EPOCH, ZoneOffset.UTC);
  private static final OffsetDateTime ZERO_RUNTIME = OffsetDateTime.ofInstant(EPOCH, ZoneOffset.UTC);

  private static final long JITTER_SEED = 21;

  @Mock
  private ScheduledExecutorService executorService;
  @Mock
  private Random random;

  private LanderMonitor underTest;

  @Mock
  private LanderTaskRunner runnable;

  @Before
  public void setUp() {
    when(runnable.getRoadName()).thenReturn(ROAD_NAME);
    when(random.nextLong()).thenReturn(JITTER_SEED);

    underTest = new LanderMonitor(clock, runnable, LoadingBay.EPOCH, executorService, random, true);
    underTest.establishLandingFrequency(PT1M);
  }

  @Test
  public void typical() {
    when(runnable.isRunning()).thenReturn(false);
    underTest.setEnabled(true);
    underTest.execute();
    verify(runnable).run(RUNTIME);
  }

  @Test
  public void checkJitterOnStartUp() {
    // First timing introduces jitter
    assertThat(underTest.getNextExecutionTime(), is(LoadingBay.EPOCH.plusMinutes(1).plusSeconds(JITTER_SEED)));
    // Second does not
    underTest.establishLandingFrequency(PT15M);
    assertThat(underTest.getNextExecutionTime(), is(LoadingBay.EPOCH.plusMinutes(15)));
  }

  @Test
  public void sufficientTimeHasPassedSinceTheLastRun() {
    underTest = new LanderMonitor(clock, runnable, OffsetDateTime.ofInstant(THREE_MINUTES_PAST_EPOCH, ZoneOffset.UTC),
        executorService, random, true);
    underTest.establishLandingFrequency(PT1M);
    when(runnable.isRunning()).thenReturn(false);
    underTest.setEnabled(true);
    underTest.execute();
    verify(runnable).run(RUNTIME);
  }

  @Test
  public void insufficientTimeHasPassedSinceTheLastRun() {
    underTest = new LanderMonitor(Clock.fixed(THREE_MINUTES_PAST_EPOCH, ZoneOffset.UTC), runnable,
        OffsetDateTime.ofInstant(FIVE_MINUTES_PAST_EPOCH, ZoneOffset.UTC), executorService, random, true);
    underTest.establishLandingFrequency(PT1M);
    when(runnable.isRunning()).thenReturn(false);
    underTest.setEnabled(true);
    underTest.execute();
    verify(runnable, never()).run(any());
  }

  @Test
  public void destinationMonitorDisabled() {
    when(runnable.isRunning()).thenReturn(false);
    underTest.setEnabled(false);
    underTest.execute();
    verify(runnable, never()).run(ZERO_RUNTIME);
  }

  @Test
  public void nextExecutionTimeNotReached() {
    underTest = new LanderMonitor(clock, runnable, LoadingBay.EPOCH, executorService, random, true);
    underTest.establishLandingFrequency(PT1M);
    when(runnable.isRunning()).thenReturn(false);
    underTest.setEnabled(true);
    underTest.execute();
    verify(runnable, never()).run(ZERO_RUNTIME);
  }

  @Test
  public void nextExecutionTimeNotSet() {
    underTest = new LanderMonitor(clock, runnable, LoadingBay.EPOCH, executorService, random, true);
    when(runnable.isRunning()).thenReturn(false);
    underTest.setEnabled(true);
    underTest.execute();
    verify(runnable, never()).run(ZERO_RUNTIME);
  }

  @Test
  public void nextExecutionTimeNotChanged() {
    underTest = new LanderMonitor(clock, runnable, LoadingBay.EPOCH, executorService, random, true);
    underTest.establishLandingFrequency(PT15M);
    OffsetDateTime nextExecutionTime = underTest.getNextExecutionTime();
    underTest.establishLandingFrequency(PT15M);
    assertThat(underTest.getNextExecutionTime(), is(nextExecutionTime));
  }

  @Test
  public void waitsForTheCurrentRunToFinish() {
    when(runnable.isRunning()).thenReturn(true);
    underTest.setEnabled(true);
    underTest.execute();
    verify(runnable, never()).run(ZERO_RUNTIME);
  }

  @Test
  public void exceptionNotPropagated() {
    when(runnable.isRunning()).thenReturn(false);
    doThrow(new RuntimeException("test-exception")).when(runnable).run(any());
    underTest.setEnabled(true);
    underTest.execute();
  }

  @Test
  public void multipleRuns() {
    when(runnable.isRunning()).thenReturn(false);
    when(runnable.run(any())).thenReturn(true, false);
    underTest.setEnabled(true);
    underTest.execute();
    verify(runnable, times(2)).run(any());
  }

  @Test
  public void startsAndShutdownsItsExecutionService() throws Exception {
    underTest = new LanderMonitor(clock, runnable, LoadingBay.EPOCH, true);
    underTest.close();
  }

  @Test
  public void tearDown() throws Exception {
    underTest.close();
  }

}
