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
package com.hotels.road.offramp.client;

import static javax.websocket.CloseReason.CloseCodes.NORMAL_CLOSURE;

import static org.glassfish.tyrus.client.ClientProperties.SSL_ENGINE_CONFIGURATOR;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.DeploymentException;
import javax.websocket.Session;
import javax.websocket.SessionException;

import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.client.SslEngineConfigurator;
import org.glassfish.tyrus.ext.client.java8.SessionBuilder;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.offramp.model.Event;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.tls.TLSConfig;

@Slf4j
class WebSocket implements AutoCloseable {
  private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);
  private final Configurator configurator;
  private final URI uri;
  private final ObjectMapper mapper;
  private final TLSConfig.Factory tlsConfigFactory;
  private final long keepAliveSeconds;
  private final EventHandler eventHandler;
  private final ErrorHandler errorHandler;
  private final CloseHandler closeHandler;
  private Session session;
  private final Disposable.Swap disposable = Disposables.swap();

  public WebSocket(
      String username,
      String password,
      URI uri,
      ObjectMapper mapper,
      TLSConfig.Factory tlsConfigFactory,
      long keepAliveSeconds,
      EventHandler eventHandler,
      ErrorHandler errorHandler,
      CloseHandler closeHandler) {
    configurator = new Configurator(username, password);
    this.uri = uri;
    this.mapper = mapper;
    this.tlsConfigFactory = tlsConfigFactory;
    this.keepAliveSeconds = keepAliveSeconds;
    this.eventHandler = eventHandler;
    this.errorHandler = errorHandler;
    this.closeHandler = closeHandler;
  }

  boolean isConnected() {
    return session != null && session.isOpen();
  }

  void connect() {
    if (!isConnected()) {
      session = connect(uri);
      disposable.update(startKeepAlives());
    }
  }

  Session connect(URI uri) {
    try {
      ClientEndpointConfig config = ClientEndpointConfig.Builder.create().configurator(configurator).build();

      ClientManager container = ClientManager.createClient();
      container.setDefaultMaxSessionIdleTimeout(Duration.ofMinutes(1).toMillis());

      if (tlsConfigFactory != null) {
        TLSConfig tlsConfig = tlsConfigFactory.create();
        SslEngineConfigurator configurator = new SslEngineConfigurator(tlsConfig.getSslContext(), true, false, false);
        configurator.setHostnameVerifier(tlsConfig.getHostnameVerifier());
        container.getProperties().put(SSL_ENGINE_CONFIGURATOR, configurator);
      }

      return new SessionBuilder(container)
          .clientEndpointConfig(config)
          .uri(uri)
          .onOpen((s, ec) -> log.info("Connected session {}", s.getId()))
          .onClose((s, cr) -> {
            log.info("Closed: {}, session: {}", cr, s.getId());
            if (cr.getCloseCode() != NORMAL_CLOSURE) {
              errorHandler.onError(new SessionException(cr.getCloseCode().toString(), null, s));
            }
            closeHandler.onClose();
          })
          .onError((s, t) -> {
            log.error("Error in session: {}", s.getId(), t);
            errorHandler.onError(t);
          })
          // Warning: Please avoid use of InputStream.class handler
          //  Under currently unknown circumstances, the use of InputStream.class
          //  handler results in erroneous triggering of the handler which ends
          //  in deserialization of garbage data. "Here be dragons"
          .messageHandler(String.class, this::receiveString)
          .messageHandler(byte[].class, this::receiveByteArray)
          .connect();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (DeploymentException e) {
      throw new RuntimeException(e);
    }
  }

  void send(Event event) {
    connect();
    log.debug("Sending Event: {}", event);
    synchronized (session) {
      try {
        session
            .getBasicRemote()
            .sendBinary(ByteBuffer.wrap(mapper.writeValueAsBytes(event)));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  void receiveString(String input) {
    try {
      Event event = mapper.readValue(input, Event.class);
      receive(event);
    } catch (IOException e) {
      log.error("Could not deserialise Event: {}", input, e);
      throw new UncheckedIOException(e);
    }
  }

  void receiveByteArray(byte[] input) {
    try {
      Event event = mapper.readValue(input, Event.class);
      receive(event);
    } catch (IOException e) {
      log.error("Could not deserialise Event", e);
      throw new UncheckedIOException(e);
    }
  }

  void receive(Event event) {
    if (log.isDebugEnabled()) {
      if (event instanceof Message) {
        log.trace("Received Message Event: {}", event);
      } else {
        log.debug("Received Event: {}", event);
      }
    }
    eventHandler.onEvent(event);
  }

  @Override
  public void close() throws Exception {
    disposable.dispose();
    if (isConnected()) {
      try {
        session.close();
        log.info("Disconnected session: {}", session.getId());
        session = null;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  // VisibleForTesting
  Disposable startKeepAlives() {
    if (keepAliveSeconds > 0) {
      return Flux.interval(Duration.ofSeconds(keepAliveSeconds)).handle((x, sink) -> {
        if (!session.isOpen()) {
          sink.complete();
        } else {
          synchronized (session) {
            try {
              log.debug("Sending Unsolicited Pong");
              session.getBasicRemote().sendPong(EMPTY);
            } catch (Exception e) {
              log.error("Failed to send Unsolicited Pong", e);
              sink.complete();
            }
          }
        }
      }).subscribe();
    }
    return Disposables.single();
  }

  interface Factory {
    WebSocket create(
        String username,
        String password,
        URI uri,
        ObjectMapper mapper,
        TLSConfig.Factory tlsConfigFactory,
        long keepAliveSeconds,
        EventHandler eventHandler,
        ErrorHandler errorHandler,
        CloseHandler closeHandler);
  }

  interface EventHandler {
    void onEvent(Event event);
  }

  interface ErrorHandler {
    void onError(Throwable throwable);
  }

  interface CloseHandler {
    void onClose();
  }
}
