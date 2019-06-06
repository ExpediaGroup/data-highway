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
package com.hotels.road.offramp.socket;

import static java.nio.charset.StandardCharsets.UTF_8;

import static lombok.AccessLevel.PACKAGE;

import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.DEFAULT_OFFSET;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.GRANTS;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.ROAD_NAME;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.STREAM_NAME;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.VERSION;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;

import org.springframework.security.core.Authentication;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.metrics.StreamMetrics;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.offramp.service.MessageFunction;
import com.hotels.road.offramp.service.OfframpService;
import com.hotels.road.offramp.service.OfframpServiceFactory;
import com.hotels.road.offramp.spi.RoadConsumer;
import com.hotels.road.rest.model.Sensitivity;

@Slf4j
@RequiredArgsConstructor
class OfframpWebSocketHandler extends AbstractWebSocketHandler {
  private final RoadConsumer.Factory consumerFactory;
  private final StreamMetrics.Factory metricsFactory;
  private final OfframpServiceFactory serviceFactory;
  private final MessageFunction.Factory messageFunctionFactory;
  private final OfframpAuthorisation authorisation;

  // These are created only afterConnectionEstablished
  private @Getter(PACKAGE) StreamMetrics metrics;
  private @Getter(PACKAGE) OfframpService service;
  private String version;
  private String roadName;
  private String streamName;
  private String sessionId;

  private final Disposable.Swap disposable = Disposables.swap();

  @Override
  public void afterConnectionEstablished(WebSocketSession session) throws Exception {
    sessionId = session.getId();

    Map<String, Object> attributes = session.getAttributes();
    version = (String) attributes.get(VERSION);
    roadName = (String) attributes.get(ROAD_NAME);
    streamName = (String) attributes.get(STREAM_NAME);
    DefaultOffset defaultOffset = (DefaultOffset) attributes.get(DEFAULT_OFFSET);
    @SuppressWarnings("unchecked")
    Set<Sensitivity> grants = (Set<Sensitivity>) attributes.get(GRANTS);
    Authentication authentication = (Authentication) session.getPrincipal();

    RoadConsumer consumer;
    metrics = metricsFactory.create(roadName, streamName);
    try {
      authorisation.checkAuthorisation(authentication, roadName, grants);
      consumer = consumerFactory.create(roadName, streamName, defaultOffset);
      MessageFunction messageFunction = messageFunctionFactory.create(roadName, grants);
      EventSender sender = bytes -> sendEvent(session, bytes);
      service = serviceFactory.create(version, consumer, messageFunction, sender, metrics);
    } catch (UnknownRoadException e) {
      metrics.markRoadNotFound();
      session.close();
      throw e;
    }

    Scheduler scheduler = Schedulers.newSingle(
        String.format("offramp[v%s,%s.%s:%s]", version, roadName, streamName, sessionId));
    metrics.incrementActiveConnections();
    disposable.update(Mono
        .fromRunnable(service)
        .subscribeOn(scheduler)
        .doOnError(t -> true, t -> close(session, consumer, scheduler, CloseStatus.SERVER_ERROR))
        .doOnSuccess(x -> close(session, consumer, scheduler, CloseStatus.NORMAL))
        .doOnTerminate(metrics::decrementActiveConnections)
        .subscribe());

    log.info("Road: {}, stream: {}, sessionId: {} - Connection established with defaultOffset: {}", roadName,
        streamName, sessionId, defaultOffset);
    metrics.markConnectionEstablished();
  }

  private void sendEvent(WebSocketSession session, String event) {
    try {
      // TODO: Remove next line and uncomment the following when move to send text websockets frames
      session.sendMessage(new BinaryMessage(event.getBytes(UTF_8)));
      // session.sendMessage(new TextMessage(event));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
    log.info("Road: {}, stream: {}, sessionId: {} - Connection closed - code: {}, reason: {}", roadName, streamName,
        sessionId, status.getCode(), status.getReason());
    disposable.dispose();
    metrics.close();
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
    if (message.getPayloadLength() == 0) {
      log.warn("Road: {}, stream: {}, sessionId: {} - Ignoring message with zero sized payload", roadName, streamName,
          sessionId);
      return;
    }
    service.onEvent(message.getPayload());
  }

  @Override
  protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
    if (message.getPayloadLength() == 0) {
      log.warn("Road: {}, stream: {}, sessionId: {} - Ignoring message with zero sized payload", roadName, streamName,
          sessionId);
      return;
    }
    service.onEvent(new String(message.getPayload().array(), UTF_8));
  }

  @Override
  protected void handlePongMessage(WebSocketSession session, PongMessage message) throws Exception {
    log.debug("Road: {}, stream: {}, sessionId: {} - Received Unsolicited Pong", roadName, streamName, sessionId);
  }

  @Override
  public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
    metrics.markTransportError();
    log.error("Road: {}, stream: {}, sessionId: {} - Transport Error", roadName, streamName, sessionId, exception);
  }

  private static void close(AutoCloseable closable) {
    try {
      closable.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void close(WebSocketSession session, RoadConsumer consumer, Scheduler scheduler, CloseStatus status) {
    try {
      close(service);
    } finally {
      try {
        close(consumer);
      } finally {
        try {
          close(() -> session.close(status));
        } finally {
          scheduler.dispose();
        }
      }
    }
  }
}
