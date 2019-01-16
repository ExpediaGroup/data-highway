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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;

import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import com.hotels.road.offramp.client.WebSocket.CloseHandler;
import com.hotels.road.offramp.client.WebSocket.ErrorHandler;
import com.hotels.road.offramp.client.WebSocket.EventHandler;
import com.hotels.road.offramp.model.Cancel;
import com.hotels.road.offramp.model.Event;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.tls.TLSConfig;

@RunWith(MockitoJUnitRunner.class)
public class WebSocketTest {
  private @Mock Session session;
  private @Mock EventHandler eventHandler;
  private @Mock ErrorHandler errorHandler;
  private @Mock CloseHandler closeHandler;

  private final URI uri = URI.create("localhost");
  private final ObjectMapper mapper = new ObjectMapper();

  private WebSocket underTest;

  @Before
  public void before() {
    TypeFactory typeFactory = mapper.getTypeFactory();
    JavaType payloadType = typeFactory.constructType(String.class);
    mapper.registerModule(Event.module(typeFactory, payloadType, String.class, null));
    underTest = spy(new WebSocket("u", "p", uri, mapper, TLSConfig.trustAllFactory(), 1L, eventHandler, errorHandler,
        closeHandler));
    doReturn(session).when(underTest).connect(uri);
  }

  @Test
  public void isConnected_sessionNull() throws Exception {
    assertThat(underTest.isConnected(), is(false));
  }

  @Test
  public void isConnected_sessionNotOpen() throws Exception {
    underTest.connect();
    doReturn(false).when(session).isOpen();
    assertThat(underTest.isConnected(), is(false));
  }

  @Test
  public void isConnected_sessionIsOpen() throws Exception {
    doReturn(true).when(session).isOpen();
    underTest.connect();
    assertThat(underTest.isConnected(), is(true));
  }

  @Test
  public void start() throws Exception {
    underTest.connect();
    verify(underTest).connect(uri);
  }

  @Test
  public void start_alreadyActive() throws Exception {
    doReturn(true).when(underTest).isConnected();
    underTest.connect();
    verify(underTest, never()).connect(uri);
  }

  @Test
  public void close() throws Exception {
    underTest.connect();
    doReturn(true).when(underTest).isConnected();
    underTest.close();
    verify(session).close();
  }

  @Test
  public void close_NotConnected() throws Exception {
    underTest.connect();
    doReturn(false).when(underTest).isConnected();
    underTest.close();
    verify(session, never()).close();
  }

  @Test(expected = UncheckedIOException.class)
  public void closeException() throws Exception {
    underTest.connect();
    doReturn(true).when(underTest).isConnected();
    doThrow(IOException.class).when(session).close();
    underTest.close();
  }

  @Test
  public void send() throws Exception {
    underTest.connect();
    RemoteEndpoint.Basic basicRemote = mock(RemoteEndpoint.Basic.class);
    doReturn(basicRemote).when(session).getBasicRemote();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    doReturn(output).when(basicRemote).getSendStream();
    underTest.send(new Cancel());
    assertThat(new String(output.toByteArray(), UTF_8), is("{\"type\":\"CANCEL\"}"));
  }

  @Test(expected = UncheckedIOException.class)
  public void sendException() throws Exception {
    underTest.connect();
    RemoteEndpoint.Basic basicRemote = mock(RemoteEndpoint.Basic.class);
    doReturn(basicRemote).when(session).getBasicRemote();
    doThrow(IOException.class).when(basicRemote).getSendStream();
    underTest.send(new Cancel());
  }

  // ====================================================
  // Test reception of websocket text frame

  @Test
  public void receiveString() throws Exception {
    Message<String> event = new Message<String>(0, 1L, 2, 3L, "message");
    String input = mapper.writeValueAsString(event);
    underTest.receiveString(input);
    verify(eventHandler).onEvent(event);
  }

  @Test(expected = UncheckedIOException.class)
  public void receiveEmptyStringException() throws Exception {
    String inputUtf8 = "";
    underTest.receiveString(inputUtf8);
  }

  // ====================================================
  // Test reception of websocket binary frame

  @Test
  public void receiveInputStream() throws Exception {
    Message<String> event = new Message<String>(0, 1L, 2, 3L, "message");
    byte[] input = mapper.writeValueAsBytes(event);
    underTest.receiveByteArray(input);
    verify(eventHandler).onEvent(event);
  }

  @Test(expected = UncheckedIOException.class)
  public void receiveEmptyInputStreamException() throws Exception {
    byte[] input = new byte[0];
    underTest.receiveByteArray(input);
  }

  // ====================================================
  // Test connection liveness

  @Test
  public void keepAlives() throws Exception {
    underTest.connect();
    RemoteEndpoint.Basic basicRemote = mock(RemoteEndpoint.Basic.class);
    doReturn(basicRemote).when(session).getBasicRemote();
    doReturn(true).when(session).isOpen();
    Awaitility.await().atMost(2, SECONDS).pollInterval(200, MILLISECONDS).until(() -> {
      try {
        verify(basicRemote).sendPong(any());
      } catch (Exception e) {
        fail();
      }
    });
  }
}
