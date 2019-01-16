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

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.hotels.road.rest.model.StandardResponse;

@Slf4j
@Getter
@Setter
public class TestMessageContext {
  private final @NonNull TestMessage message;
  private final @NonNull Duration timeout;
  private final @NonNull MessagesMetricSet metrics;

  private final BlockingQueue<Runnable> actions;

  private TestMessageContext previous;

  private boolean received = false;
  private volatile MessageState state = MessageState.CREATED;
  private long onrampTimeMs;
  private long transitTimeMs;

  TestMessageContext(
      TestMessage message,
      Duration timeout,
      MessagesMetricSet metrics,
      BlockingQueue<Runnable> actionQueue) {
    this.message = message;
    this.timeout = timeout;
    this.metrics = metrics;
    actions = actionQueue;
  }

  public void messageSent(@NonNull CompletableFuture<StandardResponse> response) {
    addAction(() -> {
      state = MessageState.SENT;
      response.handle((standardResponse, e) -> {
        message.clearPayload();
        if (state != MessageState.SENT) {
          return null;
        } else if (e != null) {
          log.info("Exception sending message: {}", e.toString());
          state = MessageState.SEND_FAILURE;
        } else if (standardResponse.isSuccess()) {
          state = MessageState.SEND_SUCCESS;
        } else {
          log.info("Message rejected by onramp: {}", standardResponse);
          state = MessageState.SEND_REJECTED;
        }
        onrampTimeMs = System.currentTimeMillis() - message.getTimestamp();
        return null;
      });
    });
  }

  public void messageReceived(@NonNull TestMessage receivedMessage) {
    checkArgument(message.getSeqNumber() == receivedMessage.getSeqNumber());
    transitTimeMs = System.currentTimeMillis() - message.getTimestamp();
    addAction(() -> {
      received = true;
      if (message.equals(receivedMessage)) {
        TestMessageContext previousMessageOnHighway = previous;
        while (previousMessageOnHighway != null && !previousMessageOnHighway.state.isOnHighway()) {
          previousMessageOnHighway = previousMessageOnHighway.previous;
        }
        if (previousMessageOnHighway == null || previousMessageOnHighway.received) {
          state = MessageState.RECEIVED_CORRECT;
        } else {
          state = MessageState.RECEIVED_OUT_OF_ORDER;
        }
      } else {
        state = MessageState.RECEIVED_CORRUPTED;
      }
      previous = null;
    });
  }

  protected void addAction(Runnable action) {
    actions.add(action);
  }

  void generateReport() {
    metrics.markMessageEndState(state);
    if (received) {
      metrics.updateTransitTime(transitTimeMs);
    }
    if (state != MessageState.CREATED && state != MessageState.SENT) {
      metrics.updateOnrampTime(onrampTimeMs);
    }
    log.debug("{} ({}ms) {}", state, transitTimeMs, message);
  }
}
