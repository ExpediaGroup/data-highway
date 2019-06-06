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
package com.hotels.road.offramp.service;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static lombok.AccessLevel.PACKAGE;

import static com.hotels.road.offramp.metrics.TimerTag.BUFFER;
import static com.hotels.road.offramp.metrics.TimerTag.COMMIT;
import static com.hotels.road.offramp.metrics.TimerTag.ENCODE;
import static com.hotels.road.offramp.metrics.TimerTag.MESSAGE;
import static com.hotels.road.offramp.metrics.TimerTag.POLL;
import static com.hotels.road.offramp.metrics.TimerTag.SEND;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.LongMath;

import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.metrics.StreamMetrics;
import com.hotels.road.offramp.model.Commit;
import com.hotels.road.offramp.model.CommitResponse;
import com.hotels.road.offramp.model.Connection;
import com.hotels.road.offramp.model.Event;
import com.hotels.road.offramp.model.Error;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.offramp.model.Rebalance;
import com.hotels.road.offramp.model.Request;
import com.hotels.road.offramp.socket.EventSender;
import com.hotels.road.offramp.spi.RoadConsumer;

@Slf4j
@RequiredArgsConstructor
public class OfframpServiceV2 implements OfframpService {
  private final RoadConsumer consumer;
  private final Encoder encoder;
  private final Function<Payload<JsonNode>, JsonNode> messageFunction;
  private final EventSender sender;
  private final StreamMetrics metrics;
  private final String podName;
  private final @Getter(PACKAGE) BlockingQueue<Event> events = new LinkedBlockingQueue<>();
  private final @Getter(PACKAGE) Queue<Record> buffer = new LinkedList<>();
  private final Semaphore semaphore = new Semaphore(0);
  private @Getter(PACKAGE) long requested = 0L;
  private volatile boolean shuttingDown = false;
  private boolean initialised = false;

  @Override
  public void run() {
    try {
      sendEvent(new Connection(podName));
      long timeout = 0L;
      while (!shuttingDown) {
        Event event = events.poll(timeout, MILLISECONDS);
        timeout = 0L;
        if (event != null) {
          handleIncomingEvent(event);
        } else if (requested > 0L) {
          if (buffer.isEmpty()) {
            replenishBuffer();
          }
          Record record = buffer.poll();
          if (record != null) {
            sendMessage(record);
          } else {
            timeout = 1L;
          }
        } else {
          timeout = 1L;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("An error occurred while serving", e);
      throw new RuntimeException(e);
    } finally {
      semaphore.release();
    }
  }

  @Override
  public void close() throws Exception {
    shuttingDown = true;
    semaphore.acquire();
  }

  @Override
  public void onEvent(String event) {
    events.offer(encoder.decode(event));
  }

  @VisibleForTesting
  void handleIncomingEvent(Event event) {
    log.debug("Received Event: {}", event);
    switch (event.getType()) {
    case REQUEST:
      handleRequest((Request) event);
      break;
    case CANCEL:
      requested = 0L;
      break;
    case COMMIT:
      handleCommit((Commit) event);
      break;
    default:
      throw new IllegalStateException("Unexpected event type: " + event);
    }
  }

  private void handleRequest(Request request) {
    if (!initialised) {
      consumer.init(request.getCount(), this::sendRebalance);
      initialised = true;
    }
    if (request.getCount() > 0) {
      requested = LongMath.saturatedAdd(requested, request.getCount());
    } else if (request.getCount() < 0) {
      sendError(
          new IllegalArgumentException(
              String.format("Requested count cannot be negative value (given %d)", request.getCount())));
    }
  }

  private void handleCommit(Commit commit) {
    boolean success = metrics.record(COMMIT, () -> consumer.commit(commit.getOffsets()));
    metrics.markCommit(success);
    sendEvent(new CommitResponse(commit.getCorrelationId(), success));
  }

  @VisibleForTesting
  void replenishBuffer() {
    Iterable<Record> records = metrics.record(POLL, () -> consumer.poll());
    metrics.record(BUFFER, () -> records.forEach(buffer::add));
  }

  @VisibleForTesting
  void sendMessage(Record record) {
    Message<JsonNode> message = metrics.record(MESSAGE, () -> {
      Payload<JsonNode> payload = record.getPayload();
      return new Message<>(record.getPartition(), record.getOffset(), payload.getSchemaVersion(),
          record.getTimestampMs(), messageFunction.apply(payload));
    });
    sendEvent(message);
    metrics.markMessageLatency(message.getPartition(), message.getTimestampMs());
    requested--;
    if (log.isDebugEnabled() && requested == 0) {
      log.debug("Completed sending all requested messages");
    }
  }

  @VisibleForTesting
  void sendRebalance(Set<Integer> assignment) {
    sendEvent(new Rebalance(assignment));
  }

  @VisibleForTesting
  void sendError(Exception exception) {
    sendEvent(new Error(exception.getMessage()));
  }

  @VisibleForTesting
  void sendEvent(Event event) {
    if (event instanceof Message) {
      if (log.isTraceEnabled()) {
        log.trace("Sending Event: {}", event);
      }
    } else {
      log.debug("Sending Event: {}", event);
    }
    String raw = encodeEvent(event);
    metrics.record(SEND, () -> sender.send(raw));
  }

  @VisibleForTesting
  String encodeEvent(Event event) {
    String raw = metrics.record(ENCODE, () -> encoder.encode(event));
    if (event instanceof Message) {
      // measure the number of bytes in message
      metrics.markMessage(raw.getBytes().length);
    }
    return raw;
  }

  @Component
  public static class Factory implements OfframpService.Factory {
    private final Encoder encoder;
    private final String podName;

    public Factory(Encoder encoder, @Value("${pod.name:unknown}") String podName) {
      this.encoder = encoder;
      this.podName = podName;
    }

    @Override
    public OfframpServiceV2 create(
        RoadConsumer consumer,
        MessageFunction messageFunction,
        EventSender sender,
        StreamMetrics metrics)
      throws UnknownRoadException {
      return new OfframpServiceV2(consumer, encoder, messageFunction, sender, metrics, podName);
    }
  }
}
