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
package com.hotels.road.highwaypatrol;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Throwables;

@Slf4j
@Component
public class TestMessageContextManager implements AutoCloseable {
  public static final int TOTAL_GROUPS = 2520;

  private final MessagesMetricSet metrics;
  private final String origin;
  private final Duration messageTimeout;
  private final int payloadSize;

  private final Map<Long, TestMessageContext> activeMessages = new HashMap<>();
  private final TestMessageContext[] partitionPreviousContext;

  private final BlockingQueue<Runnable> globalActionQueue = new LinkedBlockingQueue<>();
  private final AtomicLong seqCounter = new AtomicLong(new Random().nextLong());
  private final ScheduledExecutorService service;
  private volatile boolean shuttingDown = false;

  public TestMessageContextManager(
      @Value("#{contextWorkerService}") ScheduledExecutorService service,
      MessagesMetricSet metrics,
      @Value("${originName}") String origin,
      @Value("${messageTimeoutSeconds:60}") long messageTimeout,
      @Value("${messagePayloadSize:4096}") int payloadSize) {
    this.service = service;
    this.metrics = metrics;
    this.origin = origin;
    this.messageTimeout = Duration.ofSeconds(messageTimeout);
    this.payloadSize = payloadSize;
    partitionPreviousContext = new TestMessageContext[TOTAL_GROUPS];
  }

  @PostConstruct
  public void startActionProcessor() {
    service.execute(this::actionProcessor);
  }

  @Override
  public void close() {
    shuttingDown = true;
  }

  private void actionProcessor() {
    while (!shuttingDown) {
      try {
        Runnable action = globalActionQueue.poll(1, MILLISECONDS);
        if (action != null) {
          action.run();
        }
      } catch (InterruptedException e) {
        log.info("Interrupted");
        throw new RuntimeException(e);
      } catch (Throwable t) {
        log.error("Exception in message thread : {}", t.toString());
        Throwables.throwIfUnchecked(t);
        throw new RuntimeException(t);
      }
    }
  }

  public TestMessageContext nextContext() {
    long seqNumber = seqCounter.getAndIncrement();
    String payload = RandomStringUtils.randomAscii(payloadSize);
    int partitionNumber = Math.abs((int) (seqNumber % TOTAL_GROUPS));
    TestMessage message = new TestMessage(origin, partitionNumber, seqNumber, System.currentTimeMillis(), payload);

    TestMessageContext context = new TestMessageContext(message, messageTimeout, metrics, globalActionQueue);

    context.setPrevious(partitionPreviousContext[partitionNumber]);
    partitionPreviousContext[partitionNumber] = context;
    activeMessages.put(seqNumber, context);

    service.schedule(() -> {
      globalActionQueue.offer(() -> {
        try {
          context.generateReport();
        } finally {
          activeMessages.remove(seqNumber);
          context.setPrevious(null); // Prevent memory leak
        }
      });
    }, messageTimeout.toMillis(), MILLISECONDS);

    return context;
  }

  public TestMessageContext getContext(TestMessage message) {
    return activeMessages.remove(message.getSeqNumber());
  }
}
