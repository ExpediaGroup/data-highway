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
package com.hotels.road.offramp.socket;

import static java.nio.charset.StandardCharsets.UTF_16;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import static com.hotels.road.offramp.model.DefaultOffset.EARLIEST;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.DEFAULT_OFFSET;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.GRANTS;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.ROAD_NAME;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.STREAM_NAME;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.VERSION;
import static com.hotels.road.rest.model.Sensitivity.PII;

import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.core.Authentication;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.metrics.StreamMetrics;
import com.hotels.road.offramp.service.MessageFunction;
import com.hotels.road.offramp.service.OfframpServiceFactory;
import com.hotels.road.offramp.service.OfframpServiceV2;
import com.hotels.road.offramp.spi.RoadConsumer;


import com.hotels.road.offramp.model.Message;

@RunWith(MockitoJUnitRunner.class)
public class OfframpWebSocketHandlerTest {

  private @Mock EventSender sender;
  private @Mock RoadConsumer.Factory consumerFactory;
  private @Mock RoadConsumer consumer;
  private @Mock OfframpServiceV2.Factory serviceV2Factory;
  private @Mock OfframpServiceV2 service;
  private @Mock StreamMetrics.Factory metricsFactory;
  private @Mock StreamMetrics metrics;
  private @Mock WebSocketSession session;
  private @Mock MessageFunction.Factory messageFunctionFactory;
  private @Mock MessageFunction messageFunction;
  private @Mock OfframpAuthorisation authorisation;
  private @Mock Authentication authentication;

  private final String version = "2";
  private final String roadName = "road1";
  private final String streamName = "stream1";
  private final String sessionId = "session1";

  private OfframpServiceFactory serviceFactory;
  private OfframpWebSocketHandler underTest;

  @Before
  public void before() throws Exception {
    serviceFactory = new OfframpServiceFactory(serviceV2Factory);
    underTest = new OfframpWebSocketHandler(consumerFactory, metricsFactory, serviceFactory, messageFunctionFactory,
        authorisation);

    Map<String, Object> attributes = ImmutableMap
        .<String, Object> builder()
        .put(VERSION, version)
        .put(ROAD_NAME, roadName)
        .put(STREAM_NAME, streamName)
        .put(DEFAULT_OFFSET, EARLIEST)
        .put(GRANTS, singleton(PII))
        .build();

    doReturn(sessionId).when(session).getId();
    doReturn(attributes).when(session).getAttributes();
    doReturn(metrics).when(metricsFactory).create(roadName, streamName);
    doReturn(consumer).when(consumerFactory).create(roadName, streamName, EARLIEST);
    doReturn(messageFunction).when(messageFunctionFactory).create(roadName, singleton(PII));
    doReturn(authentication).when(session).getPrincipal();
    doReturn(service).when(serviceV2Factory).create(eq(consumer), eq(messageFunction), any(), eq(metrics));
  }

  @Test
  public void afterConnectionEstablished() throws Exception {
    underTest.afterConnectionEstablished(session);

    verify(metrics).markConnectionEstablished();
    assertThat(underTest.getService(), is(service));
    assertThat(underTest.getMetrics(), is(metrics));
  }

  @Test
  public void afterConnectionEstablished_UnknownRoadException_serviceV2() throws Exception {
    doThrow(UnknownRoadException.class).when(serviceV2Factory).create(
        eq(consumer), eq(messageFunction), any(), eq(metrics));

    try {
      underTest.afterConnectionEstablished(session);
      fail();
    } catch (UnknownRoadException e) {
      verify(metrics).markRoadNotFound();
      verify(metrics, never()).markConnectionEstablished();
      verify(session).close();
    }
  }

  @Test
  public void handleTransportError() throws Exception {
    underTest.afterConnectionEstablished(session);
    underTest.handleTransportError(session, new IOException());

    verify(metrics).markTransportError();
  }

  @Test
  public void handleBinaryMessage() throws Exception {
    Message<String> message = new Message<>(0, 1L, 1, 1L, "a");
    BinaryMessage binaryMessage = new BinaryMessage((new ObjectMapper()).writeValueAsBytes(message));
    String refUtf8 = new String(binaryMessage.getPayload().array(), UTF_8);
    String refUtf16 = new String(binaryMessage.getPayload().array(), UTF_16);

    underTest.afterConnectionEstablished(session);
    underTest.handleBinaryMessage(session, binaryMessage);

    ArgumentCaptor<String> valueCapture = ArgumentCaptor.forClass(String.class);
    verify(service).onEvent(valueCapture.capture());

    String out = valueCapture.getValue();
    assertEquals(refUtf8, out);
    assertNotEquals(refUtf16, out);
  }

  @Test
  public void handleTextMessage() throws Exception {
    Message<String> message = new Message<>(0, 1L, 1, 1L, "a");
    TextMessage textMessage = new TextMessage((new ObjectMapper()).writeValueAsString(message));
    String refUtf8 = new String(textMessage.getPayload().getBytes(), UTF_8);
    String refUtf16 = new String(textMessage.getPayload().getBytes(), UTF_16);

    underTest.afterConnectionEstablished(session);
    underTest.handleTextMessage(session, textMessage);

    ArgumentCaptor<String> valueCapture = ArgumentCaptor.forClass(String.class);
    verify(service).onEvent(valueCapture.capture());

    String out = valueCapture.getValue();
    assertEquals(refUtf8, out);
    assertNotEquals(refUtf16, out);
  }
}
