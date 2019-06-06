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
package com.hotels.road.offramp.client;

import static lombok.AccessLevel.PACKAGE;
import static reactor.core.publisher.DirectProcessor.create;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import org.reactivestreams.Publisher;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.DefaultRetry;
import reactor.retry.Retry;

import com.hotels.road.offramp.model.CommitResponse;
import com.hotels.road.offramp.model.Connection;
import com.hotels.road.offramp.model.Event;
import com.hotels.road.offramp.model.Error;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.offramp.model.Rebalance;

@Slf4j
class OfframpClientImpl<T> implements OfframpClient<T> {
  private final @Getter(PACKAGE) MessageProcessor<T> messages;
  private final CommitHandler commitHandler;
  private final @Getter(PACKAGE) DirectProcessor<Rebalance> rebalances = create();
  private final OfframpOptions<T> options;
  private final WebSocket socket;

  OfframpClientImpl(OfframpOptions<T> options) {
    this(options, WebSocket::new, CommitHandler::new);
  }

  OfframpClientImpl(
      OfframpOptions<T> options,
      WebSocket.Factory socketFactory,
      CommitHandler.Factory commitHandlerFactory) {
    this.options = options;
    messages = new MessageProcessor<>(this::send);
    commitHandler = commitHandlerFactory.create(this::send);
    socket = socketFactory.create(options.getUsername(), options.getPassword(), options.uri(), options.objectMapper(),
        options.getTlsConfigFactory(), options.getKeepAliveSeconds(), this::onNext, messages::onError,
        commitHandler::onClose);
  }

  @Override
  public Publisher<Message<T>> messages() {
    Flux<Message<T>> flux = Flux.from(messages);
    if (options.getInitialRequestAmount() != Integer.MAX_VALUE) {
      flux = flux.limitRate(options.getInitialRequestAmount(), options.getReplenishingRequestAmount());
    }
    return flux.retryWhen(retryStrategy());
  }

  @Override
  public Mono<Boolean> commit(Map<Integer, Long> offsets) {
    return commitHandler.commit(offsets);
  }

  @Override
  public Publisher<Set<Integer>> rebalances() {
    return rebalances.map(Rebalance::getAssignment);
  }

  @Override
  public void close() throws Exception {
    socket.close();
  }

  @SuppressWarnings("unchecked")
  void onNext(Event event) {
    if (event instanceof Message) {
      messages.onNext((Message<T>) event);
    } else if (event instanceof CommitResponse) {
      commitHandler.complete((CommitResponse) event);
    } else if (event instanceof Rebalance) {
      rebalances.onNext((Rebalance) event);
    } else if (event instanceof Connection) {
      log.info("Connected to agent {}", ((Connection) event).getAgentName());
    } else if (event instanceof Error) {
      log.error(((Error) event).getReason());
    } else {
      log.warn("Unknown message type: {}", event);
    }
  }

  void send(Event event) {
    socket.send(event);
  }

  private Retry<?> retryStrategy() {
    return DefaultRetry.create(c -> options.isRetry()).retryMax(Integer.MAX_VALUE).fixedBackoff(Duration.ofSeconds(1));
  }
}
