/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
import static java.util.concurrent.TimeUnit.MINUTES;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.joda.time.format.ISODateTimeFormat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import com.hotels.road.hive.metastore.HivePartitionManager;
import com.hotels.road.hive.metastore.MetaStoreException;
import com.hotels.road.loadingbay.event.HiveNotificationHandler;
import com.hotels.road.loadingbay.lander.Lander;
import com.hotels.road.loadingbay.lander.LanderConfiguration;
import com.hotels.road.loadingbay.lander.OffsetRange;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@Slf4j
public class LanderTaskRunner {
  public static final String ACQUISITION_INSTANT = "acquisition_instant";
  public static final String REVISION_INSTANT = "revision_instant";
  private static final String LAST_RUN_PATH = "/destinations/hive/status/lastRun";
  private final OffsetManager offsetManager;
  private final String topicName;
  private final String database;
  private final String roadName;
  private final HivePartitionManager hivePartitionManager;

  private final Timer landingTimer;
  private final Counter partitionMutationCounter;
  private final Counter metaStoreErrorMeter;
  private final Counter messagesLandedCounter;
  private final boolean enableServerSideEncryption;

  private final Lander.Factory landerFactory;
  private final HiveNotificationHandler landingHandler;
  private final PatchSetEmitter emitter;
  private final Clock clock;
  private final long maxRecordsPerPartition;
  private final int landingTimeoutMinutes;
  private volatile State state;

  public LanderTaskRunner(
      MeterRegistry registry,
      OffsetManager offsetManager,
      String roadName,
      String topicName,
      String database,
      HivePartitionManager hivePartitionManager,
      Lander.Factory landerFactory,
      HiveNotificationHandler landingHandler,
      PatchSetEmitter emitter,
      Clock clock,
      long maxRecordsPerPartition,
      boolean enableServerSideEncryption,
      int landingTimeoutMinutes) {
    this.offsetManager = offsetManager;
    this.roadName = roadName;
    this.topicName = topicName;
    this.database = database;
    this.hivePartitionManager = hivePartitionManager;
    this.landerFactory = landerFactory;
    this.landingHandler = landingHandler;
    this.emitter = emitter;
    this.clock = clock;
    this.maxRecordsPerPartition = maxRecordsPerPartition;
    this.enableServerSideEncryption = enableServerSideEncryption;
    this.landingTimeoutMinutes = landingTimeoutMinutes;
    landingTimer = Timer
        .builder("loading-bay.landing-time")
        .tag("road", roadName)
        .publishPercentileHistogram()
        .register(registry);
    partitionMutationCounter = registry.counter("loading-bay.partition-mutations", "road", roadName);
    metaStoreErrorMeter = registry.counter("loading-bay.meta-store-errors", "road", roadName);
    messagesLandedCounter = registry.counter("loading-bay.messages-landed", "road", roadName);
    changeState(State.IDLE);
  }

  State getState() {
    return state;
  }

  String getRoadName() {
    return roadName;
  }

  public boolean run(OffsetDateTime runtimeDateTime) {
    emitter
        .emit(new PatchSet(roadName, singletonList(PatchOperation.replace(LAST_RUN_PATH, runtimeDateTime.toString()))));
    String acquisitionInstant = ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC().print(
        runtimeDateTime.toInstant().toEpochMilli());

    changeState(State.PREPARING);
    log.info("Preparing to land partition {}.", acquisitionInstant);

    boolean runAgain = landingTimer.record(() -> runChain(acquisitionInstant));
    log.info("Update message for road {}: landerLastRun: {}, runAgain: {}", roadName, runtimeDateTime, runAgain);
    return runAgain;
  }

  boolean runChain(String acquisitionInstant) {
    try {
      LanderConfiguration landerConfiguration = prepareLanderConfiguration(acquisitionInstant);
      CompletableFuture<LanderConfiguration> future = landerFactory.newInstance(landerConfiguration).run();
      try {
        future.get(landingTimeoutMinutes, MINUTES);
        updateMetadata(landerConfiguration);
        long totalMessages = landerConfiguration
            .getOffsets()
            .values()
            .stream()
            .mapToLong(r -> r.getEnd() - r.getStart())
            .sum();
        messagesLandedCounter.increment(totalMessages);
      } catch (TimeoutException e) {
        log.warn("Landing of {}, {} timed out", landerConfiguration.getRoadName(), acquisitionInstant);
        future.cancel(true);
        return true;
      } catch (MetaStoreException e) {
        return true;
      } finally {
        changeState(State.IDLE);
      }

      return landerConfiguration.isRunAgain();
    } catch (Throwable t) {
      onException(acquisitionInstant, t);
    }
    return false;
  }

  void onException(String acquisitionInstant, Throwable t) {
    if (t instanceof NoDataToLandException) {
      log.info("Last landing '{}' found no data", acquisitionInstant);
      changeState(State.IDLE);
      return;
    }
    log.info("Error landing partition {} - {}", acquisitionInstant, t.getMessage());
    log.error("Problem landing data", t);
    changeState(State.IDLE);
  }

  LanderConfiguration prepareLanderConfiguration(String acquisitionInstant) {
    Map<Integer, Long> comittedOffsets = offsetManager.getCommittedOffsets(topicName);
    Map<Integer, OffsetRange> offsets = new HashMap<>();
    boolean runAgain = false;
    for (Entry<Integer, Long> entry : offsetManager.getLatestOffsets(topicName).entrySet()) {
      Integer partition = entry.getKey();
      long committedOffset = comittedOffsets.getOrDefault(partition, 0L);
      long latestOffset = entry.getValue();
      if (latestOffset > committedOffset) {
        if (latestOffset > committedOffset + maxRecordsPerPartition) {
          latestOffset = committedOffset + maxRecordsPerPartition;
          runAgain = true;
        }
        offsets.put(partition, new OffsetRange(committedOffset, latestOffset));
      }
    }

    if (offsets.isEmpty()) {
      throw new NoDataToLandException();
    }

    String s3KeyPrefix = String.format("%s/%s/%d/%s=%s", database, roadName, clock.millis(), ACQUISITION_INSTANT,
        acquisitionInstant);

    changeState(State.LANDING);
    log.info("Landing partition {}.", acquisitionInstant);
    return new LanderConfiguration(roadName, topicName, offsets, s3KeyPrefix, enableServerSideEncryption,
        acquisitionInstant, runAgain);
  }

  void updateMetadata(LanderConfiguration config) {
    changeState(State.UPDATING);
    String acquisitionInstant = config.getAcquisitionInstant();
    log.info("Updating table to add partition {}, {}.", config.getRoadName(), acquisitionInstant);

    String partitionSpec = ACQUISITION_INSTANT + "=" + acquisitionInstant;
    List<String> partitionValues = singletonList(acquisitionInstant);
    try {
      Optional<Partition> partition = hivePartitionManager.addPartition(roadName, partitionValues,
          config.getS3KeyPrefix());
      Map<Integer, Long> offsets = new HashMap<>();
      config.getOffsets().forEach((pid, range) -> offsets.put(pid, range.getEnd()));
      offsetManager.commitOffsets(topicName, offsets);
      if (partition.isPresent()) {
        long recordCount = config.getOffsets().values().stream().mapToLong(r -> r.getEnd() - r.getStart()).sum();
        landingHandler.handlePartitionCreated(roadName, partition.get(), partitionSpec, recordCount);
      } else {
        // Partition already exists
        partitionMutationCounter.increment();
        log.warn("Data landed into existing partition; road={} partitionSpec={}", roadName, partitionSpec);
      }
    } catch (MetaStoreException e) {
      metaStoreErrorMeter.increment();
      throw e;
    }
  }

  void changeState(State state) {
    log.info("State change : {}:{}", roadName, state);
    this.state = state;
  }

  boolean isRunning() {
    return State.IDLE != state;
  }

  enum State {
    IDLE,
    PREPARING,
    LANDING,
    UPDATING
  }
}
