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
package com.hotels.road.highwaypatrol;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.hotels.road.client.AsyncRoadClient;
import com.hotels.road.rest.model.StandardResponse;

@Slf4j
@Component
public class Sender implements AutoCloseable {
  private final ScheduledExecutorService service;

  private final AsyncRoadClient<TestMessage> onrampClient;
  private final TestMessageContextManager contextManager;
  private final long messageRate;

  @Autowired
  public Sender(
      AsyncRoadClient<TestMessage> onrampClient,
      TestMessageContextManager contextManager,
      @Value("${messageHz}") long messageRate) {
    this(onrampClient, contextManager, messageRate,
        Executors.newScheduledThreadPool(4, new ThreadFactoryBuilder().setNameFormat("sender-%d").build()));
  }

  Sender(
      AsyncRoadClient<TestMessage> onrampClient,
      TestMessageContextManager contextManager,
      long messageRate,
      ScheduledExecutorService service) {
    this.messageRate = messageRate;
    checkArgument(messageRate > 0, "Message frequency (messageHz) must be greater than 0Hz");
    checkArgument(messageRate <= 1000, "Message frequency (messageHz) must be 1000Hz or below");
    checkArgument(1000 % messageRate == 0, "Message frequency value (messageHz) must be a factor of 1000");

    this.onrampClient = onrampClient;
    this.contextManager = contextManager;
    this.service = service;
  }

  public void start() {
    log.info("Starting message sender at {}Hz", messageRate);
    service.scheduleAtFixedRate(this::worker, 0, 1000 / messageRate, MILLISECONDS);
  }

  public void worker() {
    try {
      TestMessageContext context = contextManager.nextContext();

      // Send the message
      CompletableFuture<StandardResponse> response = onrampClient.sendMessage(context.getMessage());
      context.messageSent(response);
    } catch (Throwable t) {
      log.warn("Problem creating and sending message", t);
    }
  }

  @Override
  public void close() throws Exception {
    service.shutdown();
    if (!service.awaitTermination(5, TimeUnit.MINUTES)) {
      throw new Exception("Timed out waiting for sender service to terminate");
    }
    log.info("Shutdown sender");
  }
}
