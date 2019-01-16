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
package com.hotels.road.tollbooth.app;

import java.io.IOException;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.tollbooth.client.api.PatchSet;

@Slf4j
@Component
public class TollBooth extends HealthCheck implements AutoCloseable {
  private final ObjectMapper mapper;
  private final Consumer<String, String> patchConsumer;
  private final PatchProcessor patchProcessor;

  private final Thread thread;
  private Throwable exceptionalShutdownReason;
  private boolean shuttingDown = false;

  @Autowired
  public TollBooth(ObjectMapper mapper, Consumer<String, String> patchConsumer, PatchProcessor patchProcessor) {
    this.mapper = mapper;
    this.patchConsumer = patchConsumer;
    this.patchProcessor = patchProcessor;

    thread = new Thread(this::processPatches, "tollbooth-worker");
    thread.start();
  }

  @Override
  protected Result check() throws Exception {
    if (thread.isAlive()) {
      return Result.healthy();
    } else {
      if (exceptionalShutdownReason != null) {
        return Result.unhealthy(exceptionalShutdownReason);
      } else {
        return Result.unhealthy("Patch processing thread is not running");
      }
    }
  }

  private void processPatches() {
    try {
      while (!shuttingDown) {
        patchConsumer.poll(100).forEach(record -> processPatchSet(record.value()));
      }
    } catch (Throwable t) {
      log.warn("Patch processing thread has died", t);
      exceptionalShutdownReason = t;
    }
  }

  private void processPatchSet(String patchSetJson) {
    try {
      log.info(patchSetJson);
      PatchSet patchSet = mapper.readValue(patchSetJson, PatchSet.class);
      patchProcessor.processPatch(patchSet);
    } catch (PatchApplicationException | IOException e) {
      log.error("Error applying patch to document: {}", patchSetJson, e);
    }
  }

  @Override
  public void close() throws Exception {
    shuttingDown = true;
    thread.join();
  }
}
