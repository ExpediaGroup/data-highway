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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.RequiredArgsConstructor;
import lombok.Value;

public class OfframpMetrics {
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

  private final MeterRegistry registry;
  private final Clock clock;
  private final Tags roadStreamTags;
  private final Counter messageCounter;
  private final Counter bytesCounter;
  private final Counter rebalanceCounter;
  private final Counter roadNotFoundCounter;
  private final Counter commitSuccessCounter;
  private final Counter commitFailureCounter;
  private final Counter transportErrorCounter;
  private final Counter connectionEstablishedCounter;
  private final RoadAndStream roadAndStream;
  private final Map<RoadAndStream, AtomicInteger> activeConnections;
  private final ConcurrentMap<Integer, AtomicLong> partitionLatencies;

  OfframpMetrics(
      String roadName,
      String streamName,
      MeterRegistry registry,
      Clock clock,
      Map<RoadAndStream, AtomicInteger> activeConnections) {
    this.registry = registry;
    this.clock = clock;
    this.activeConnections = activeConnections;
    roadAndStream = RoadAndStream.of(roadName, streamName);
    roadStreamTags = Tags.of(ROAD, roadName).and(STREAM, streamName);
    commitSuccessCounter = registry.counter(OFFRAMP + COMMIT_SUCCESS, roadStreamTags);
    commitFailureCounter = registry.counter(OFFRAMP + COMMIT_FAILURE, roadStreamTags);
    messageCounter = registry.counter(OFFRAMP + MESSAGE, roadStreamTags);
    bytesCounter = registry.counter(OFFRAMP + BYTES, roadStreamTags);
    rebalanceCounter = registry.counter(OFFRAMP + REBALANCE, roadStreamTags);
    roadNotFoundCounter = registry.counter(OFFRAMP + ROAD_NOT_FOUND, roadStreamTags);
    transportErrorCounter = registry.counter(OFFRAMP + TRANSPORT_ERROR, roadStreamTags);
    connectionEstablishedCounter = registry.counter(OFFRAMP + CONNECTIONS_ESTABLISHED, roadStreamTags);
    partitionLatencies = new ConcurrentHashMap<>();
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
    getPartitionLatencyHolder(partition).set(clock.millis() - timestamp);
  }

  private AtomicLong getPartitionLatencyHolder(int partition) {
    return partitionLatencies.computeIfAbsent(partition, k -> {
      Tags tags = roadStreamTags.and("partition", Integer.toString(k));
      AtomicLong latencyHolder = new AtomicLong();
      registry.more().timeGauge(OFFRAMP + LATENCY, tags, latencyHolder, MILLISECONDS, AtomicLong::doubleValue);
      return latencyHolder;
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
    registry.timer(OFFRAMP_TIMER, roadStreamTags.and(timerTag.tag)).record(runnable);
  }

  public <T> T record(TimerTag timerTag, Supplier<T> supplier) {
    return registry.timer(OFFRAMP_TIMER, roadStreamTags.and(timerTag.tag)).record(supplier);
  }

  public void incrementActiveConnections() {
    activeConnections
        .computeIfAbsent(roadAndStream,
            x -> registry.gauge(OFFRAMP + ACTIVE_CONNECTIONS, roadStreamTags, new AtomicInteger()))
        .incrementAndGet();
  }

  public void decrementActiveConnections() {
    activeConnections.get(roadAndStream).decrementAndGet();
  }

  @Component
  @RequiredArgsConstructor
  public static class Factory {
    private final MeterRegistry registry;
    private final Clock clock;
    private final Map<RoadAndStream, AtomicInteger> activeConnections = new ConcurrentHashMap<>();

    public OfframpMetrics create(String roadName, String streamName) {
      return new OfframpMetrics(roadName, streamName, registry, clock, activeConnections);
    }
  }

  public enum TimerTag {
    COMMIT,
    POLL,
    BUFFER,
    SEND,
    ENCODE,
    MESSAGE;

    public final Tag tag = Tag.of("event", name());
  }

  @Value
  @RequiredArgsConstructor(staticName = "of")
  static class RoadAndStream {
    String road;
    String stream;
  }
}
