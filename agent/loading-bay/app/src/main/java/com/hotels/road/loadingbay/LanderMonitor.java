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

import static java.lang.Math.abs;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;

import com.google.common.annotations.VisibleForTesting;

@Slf4j
public class LanderMonitor implements AutoCloseable {
  private final Clock clock;
  private final LanderTaskRunner landerTaskRunner;
  private volatile OffsetDateTime lastRun;
  private final ScheduledExecutorService executorService;
  private volatile boolean enabled = false;
  private volatile OffsetDateTime nextExecutionTime = null;
  private volatile Duration landingDuration;
  private volatile Random random;
  private final boolean jitter;

  public LanderMonitor(Clock clock, LanderTaskRunner landerTaskRunner, OffsetDateTime lastRun, boolean jitter) {
    this(clock, landerTaskRunner, lastRun, Executors.newScheduledThreadPool(1), new Random(), jitter);
  }

  public LanderMonitor(
      Clock clock,
      LanderTaskRunner landerTaskRunner,
      OffsetDateTime lastRun,
      ScheduledExecutorService executorService,
      Random random,
      boolean jitter) {
    this.clock = clock;
    this.landerTaskRunner = landerTaskRunner;
    this.lastRun = lastRun;
    this.random = random;
    this.executorService = executorService;
    this.jitter = jitter;
    this.executorService.scheduleAtFixedRate(this::execute, 1, 10, SECONDS);
    this.executorService.scheduleAtFixedRate(this::printLanderMonitorStatus, 1, 5, MINUTES);
  }

  public void setEnabled(boolean enabled) {
    if (this.enabled == enabled) {
      return;
    }
    log.info("Setting enabled for {} to {}", landerTaskRunner.getRoadName(), enabled);
    this.enabled = enabled;
  }

  public void establishLandingFrequency(String landingInterval) {
    Duration landingDuration = Duration.parse(landingInterval);
    if (Objects.equals(this.landingDuration, landingDuration)) {
      return;
    }
    Duration jitterDuration = Duration.ZERO;
    if (nextExecutionTime == null) {
      long jitterSeconds = 0L;
      if (jitter) {
        jitterSeconds = abs(random.nextLong()) % landingDuration.getSeconds();
      }
      log.info("Introducing start-up jitter of {} millis.", SECONDS.toMillis(jitterSeconds));
      jitterDuration = Duration.ofSeconds(jitterSeconds);
    }
    nextExecutionTime = lastRun.plus(landingDuration).plus(jitterDuration);
    log.info("New landing interval for {} is {} ({} millis). Next execution time based on landerLastRun {} is {}",
        landerTaskRunner.getRoadName(), landingInterval, landingDuration.toMillis(), lastRun, nextExecutionTime);
    this.landingDuration = landingDuration;
  }

  @Override
  public void close() throws Exception {
    executorService.shutdown();
    executorService.awaitTermination(5, MINUTES);
  }

  @VisibleForTesting
  OffsetDateTime getNextExecutionTime() {
    return nextExecutionTime;
  }

  @VisibleForTesting
  void execute() {
    try {
      if (!landerTaskRunner.isRunning()
          && enabled
          && nextExecutionTime != null
          && nextExecutionTime.isBefore(OffsetDateTime.now(clock))) {
        boolean runAgain = true;
        while (runAgain) {
          lastRun = OffsetDateTime.now(clock);
          runAgain = landerTaskRunner.run(lastRun);
        }
        nextExecutionTime = lastRun.plus(landingDuration);
        log.info("Setting next execution time for road {} to {}", landerTaskRunner.getRoadName(), nextExecutionTime);
      }
    } catch (Exception e) {
      log.error("Exception running execute", e);
    }
  }

  private void printLanderMonitorStatus() {
    log.info("Road {} status: LanderTaskRunner running: {}, road enabled: {}, last run: {}, next run: {}",
        landerTaskRunner.getRoadName(), landerTaskRunner.isRunning(), enabled, lastRun, nextExecutionTime);
  }

}
