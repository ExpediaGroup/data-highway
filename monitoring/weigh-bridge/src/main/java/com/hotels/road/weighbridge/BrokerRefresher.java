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
package com.hotels.road.weighbridge;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import com.hotels.road.weighbridge.model.Broker;

@Component
@Slf4j
public class BrokerRefresher implements ApplicationRunner, AutoCloseable {

  private final Duration refreshPeriod;
  private final BrokerSupplier brokerSupplier;
  private final WeighBridgeMetrics metrics;
  private final AtomicReference<Broker> brokerReference;
  private final Map<Integer, Broker> map;

  private final ScheduledExecutorService executor;
  private ScheduledFuture<?> scheduledFuture;

  public BrokerRefresher(
      @Value("${refreshPeriod:PT10S}") Duration refreshPeriod,
      BrokerSupplier brokerSupplier,
      WeighBridgeMetrics metrics,
      AtomicReference<Broker> brokerReference,
      Map<Integer, Broker> map) {
    this.refreshPeriod = refreshPeriod;
    this.brokerSupplier = brokerSupplier;
    this.metrics = metrics;
    this.brokerReference = brokerReference;
    this.map = map;

    executor = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    scheduledFuture = executor.scheduleAtFixedRate(this::updateMetadata, 0, refreshPeriod.toMillis(), MILLISECONDS);
  }

  private void updateMetadata() {
    try {
      Broker broker = brokerSupplier.get();
      map.put(broker.getId(), broker);
      metrics.update(broker);
      brokerReference.set(broker);
    } catch (Throwable t) {
      log.info("Problem updating broker metadata", t);
    }
  }

  @Override
  public void close() throws Exception {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }
    executor.shutdown();
  }
}
