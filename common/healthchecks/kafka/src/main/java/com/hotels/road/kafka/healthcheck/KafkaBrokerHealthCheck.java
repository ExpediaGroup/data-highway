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
package com.hotels.road.kafka.healthcheck;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;

@Component
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
public class KafkaBrokerHealthCheck extends HealthCheck implements AutoCloseable {

  private static final String GROUP_ID = "healthcheck";

  private final Consumer<String, String> consumer;

  private final ExecutorService executorService;

  @Autowired
  public KafkaBrokerHealthCheck(KafkaConsumerFactory kafkaConsumerFactory) {
    this(kafkaConsumerFactory.create(GROUP_ID), Executors.newFixedThreadPool(1));
  }

  @Override
  protected Result check() throws Exception {
    try {
      return executorService.submit(this::isBrokerMetadataAccessible).get(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      log.error("TimeoutException occurred whilst fetching broker metadata {}", e);
      return Result.unhealthy("Timeout occured whilst fetching broker's metadata.");
    } catch (InterruptedException | ExecutionException e) {
      log.error("An error occurred whilst fetching broker metadata {}", e);
      return Result.unhealthy(e);
    }
  }

  @VisibleForTesting
  Result isBrokerMetadataAccessible() {
    try {
      consumer.listTopics();
      return Result.healthy();
    } catch (Exception e) {
      log.error("Error occurred whilst fetching broker metadata {}", e);
      return Result.unhealthy(e);
    }
  }

  @Override
  public void close() {
    try {
      log.info("Closing KafkaBrokerHealthCheck..");
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("InterruptedException occured, {}", e);
      // ignore
    }
  }

}
