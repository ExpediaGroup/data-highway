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

import java.time.Clock;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;

public class StreamMetrics implements AutoCloseable {
  static final String ROAD = "road";
  static final String STREAM = "stream";

  static final String OFFRAMP = "offramp.";
  static final String BYTES = "bytes";
  static final String MESSAGE = "message";
  static final String LATENCY = "latency";
  static final String COMMIT_SUCCESS = "commit.success";
  static final String COMMIT_FAILURE = "commit.failure";
  static final String REBALANCE = "rebalance";
  static final String ROAD_NOT_FOUND = "road-not-found";
  static final String TRANSPORT_ERROR = "transport-error";
  static final String CONNECTIONS_ESTABLISHED = "connections-established";
  static final String ACTIVE_CONNECTIONS = "active-connections";
  private static final String OFFRAMP_TIMER = "offramp_timer";

  private final MeterPool pool;
  private final Clock clock;
  private final Tags roadStreamTags;

  private final Counter commitSuccessCounter;
  private final Counter commitFailureCounter;
  private final Counter messageCounter;
  private final Counter bytesCounter;
  private final Counter rebalanceCounter;
  private final Counter roadNotFoundCounter;
  private final Counter transportErrorCounter;
  private final Counter connectionEstablishedCounter;
  private final SettableGauge activeConnections;
  private final ConcurrentMap<Integer, SettableTimeGauge> partitionLatencies;
  private final Map<TimerTag, Timer> timers;

  StreamMetrics(String roadName, String streamName, MeterPool pool, Clock clock) {
    this.pool = pool;
    this.clock = clock;

    roadStreamTags = Tags.of(ROAD, roadName).and(STREAM, streamName);

    commitSuccessCounter = pool.takeCounter(OFFRAMP + COMMIT_SUCCESS, roadStreamTags);
    commitFailureCounter = pool.takeCounter(OFFRAMP + COMMIT_FAILURE, roadStreamTags);
    messageCounter = pool.takeCounter(OFFRAMP + MESSAGE, roadStreamTags);
    bytesCounter = pool.takeCounter(OFFRAMP + BYTES, roadStreamTags);
    rebalanceCounter = pool.takeCounter(OFFRAMP + REBALANCE, roadStreamTags);
    roadNotFoundCounter = pool.takeCounter(OFFRAMP + ROAD_NOT_FOUND, roadStreamTags);
    transportErrorCounter = pool.takeCounter(OFFRAMP + TRANSPORT_ERROR, roadStreamTags);
    connectionEstablishedCounter = pool.takeCounter(OFFRAMP + CONNECTIONS_ESTABLISHED, roadStreamTags);

    activeConnections = pool.takeGauge(OFFRAMP + ACTIVE_CONNECTIONS, roadStreamTags);

    partitionLatencies = new ConcurrentHashMap<>();

    timers = new EnumMap<>(TimerTag.class);
    for (TimerTag tag : TimerTag.values()) {
      timers.put(tag, pool.takeTimer(OFFRAMP_TIMER, roadStreamTags.and(tag.tag)));
    }
  }

  public void markCommit(boolean success) {
    if (success) {
      commitSuccessCounter.increment();
    } else {
      commitFailureCounter.increment();
    }
  }

  public void markMessage(long bytes) {
    messageCounter.increment();
    bytesCounter.increment(bytes);
  }

  public void markMessageLatency(int partition, long timestamp) {
    getPartitionLatencyHolder(partition).setValue(clock.millis() - timestamp);
  }

  @Override
  public void close() throws Exception {
    pool.returnCounter(commitSuccessCounter);
    pool.returnCounter(commitFailureCounter);
    pool.returnCounter(messageCounter);
    pool.returnCounter(bytesCounter);
    pool.returnCounter(rebalanceCounter);
    pool.returnCounter(roadNotFoundCounter);
    pool.returnCounter(transportErrorCounter);
    pool.returnCounter(connectionEstablishedCounter);
    pool.returnGauge(activeConnections);
    partitionLatencies.values().forEach(pool::returnTimeGauge);
    timers.values().forEach(pool::returnTimer);
  }

  private SettableTimeGauge getPartitionLatencyHolder(int partition) {
    return partitionLatencies.computeIfAbsent(partition, k -> {
      String name = OFFRAMP + LATENCY;
      Tags tags = roadStreamTags.and("partition", Integer.toString(k));

      return pool.takeTimeGauge(name, tags);
    });
  }

  public void markRebalance() {
    rebalanceCounter.increment();
  }

  public void markRoadNotFound() {
    roadNotFoundCounter.increment();
  }

  public void markTransportError() {
    transportErrorCounter.increment();
  }

  public void markConnectionEstablished() {
    connectionEstablishedCounter.increment();
  }

  public void record(TimerTag timerTag, Runnable runnable) {
    timers.get(timerTag).record(runnable);
  }

  public <T> T record(TimerTag timerTag, Supplier<T> supplier) {
    return timers.get(timerTag).record(supplier);
  }

  public void incrementActiveConnections() {
    activeConnections.increment();
  }

  public void decrementActiveConnections() {
    activeConnections.decrement();
  }

  @Component
  @RequiredArgsConstructor
  public static class Factory {
    private final MeterPool pool;
    private final Clock clock;

    public StreamMetrics create(String roadName, String streamName) {
      return new StreamMetrics(roadName, streamName, pool, clock);
    }
  }
}
