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

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static reactor.core.scheduler.Schedulers.single;

import static com.hotels.road.offramp.metrics.TimerTag.BUFFER;
import static com.hotels.road.offramp.metrics.TimerTag.COMMIT;
import static com.hotels.road.offramp.metrics.TimerTag.ENCODE;
import static com.hotels.road.offramp.metrics.TimerTag.MESSAGE;
import static com.hotels.road.offramp.metrics.TimerTag.POLL;
import static com.hotels.road.offramp.metrics.TimerTag.SEND;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import reactor.core.publisher.Mono;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.metrics.StreamMetrics;
import com.hotels.road.offramp.model.Cancel;
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
import com.hotels.road.offramp.spi.RoadConsumer.RebalanceListener;

@RunWith(MockitoJUnitRunner.class)
public class OfframpServiceV2Test {
  private @Mock RoadConsumer consumer;
  private @Mock Encoder encoder;
  private @Mock MessageFunction messageFunction;
  private @Mock EventSender sender;
  private @Mock StreamMetrics metrics;

  private final ObjectMapper mapper = new ObjectMapper();
  private final String podName = "podName";

  private OfframpServiceV2 underTest;

  @Before
  public void before() throws Exception {
    underTest = spy(new OfframpServiceV2(consumer, encoder, messageFunction, sender, metrics, podName));
  }

  @Test
  public void run() throws Exception {
    Request request = new Request(1L);
    JsonNode value = mapper.createObjectNode();
    Payload<JsonNode> payload = new Payload<JsonNode>((byte) 0, 2, value);
    Record record = new Record(0, 1L, 3L, payload);

    doNothing().when(underTest).sendEvent(any());
    doNothing().when(underTest).sendRebalance(any());

    doAnswer(i -> {
      underTest.getBuffer().offer(record);
      return null;
    }).when(underTest).replenishBuffer();

    try {
      Mono.fromRunnable(underTest).subscribeOn(single()).subscribe();

      underTest.getEvents().offer(request);
      Awaitility.await().atMost(500, MILLISECONDS).pollInterval(10, MILLISECONDS).until(() -> {
        verify(underTest).handleIncomingEvent(request);
      });

      Awaitility.await().atMost(500, MILLISECONDS).pollInterval(10, MILLISECONDS).until(() -> {
        verify(underTest).sendMessage(record);
      });

    } finally {
      underTest.close();
    }

    verify(underTest).sendEvent(new Connection(podName));
    ArgumentCaptor<RebalanceListener> captor = ArgumentCaptor.forClass(RebalanceListener.class);
    verify(consumer).init(eq(1L), captor.capture());

    Set<Integer> assignment = singleton(0);
    captor.getValue().onRebalance(assignment);
    verify(underTest).sendRebalance(assignment);
  }

  @Test
  public void onEvent_testWithString() throws Exception {
    Event event = new Cancel();
    String raw = mapper.writeValueAsString(event);

    doReturn(event).when(encoder).decode(raw);

    underTest.onEvent(raw);

    assertThat(underTest.getEvents().peek(), is(event));
  }

  @Test
  public void handleIncomingRequest() throws Exception {
    underTest.handleIncomingEvent(new Request(10L));
    assertThat(underTest.getRequested(), is(10L));
  }

  @Test
  public void handleIncomingRequestCountZero() throws Exception {
    underTest.handleIncomingEvent(new Request(0L));
    assertThat(underTest.getRequested(), is(0L));
  }

  @Test
  public void handleIncomingRequestCountNegative() throws Exception {
    underTest.handleIncomingEvent(new Request(-1000L));
    assertThat(underTest.getRequested(), is(0L));
    Exception ex = new IllegalArgumentException("Requested count cannot be negative value (given -1000)");
    verify(underTest).sendEvent(new Error(ex.getMessage()));
  }

  @Test
  public void handleIncomingCancel() throws Exception {
    underTest.handleIncomingEvent(new Request(10L));
    underTest.handleIncomingEvent(new Cancel());
    assertThat(underTest.getRequested(), is(0L));
  }

  @Test
  public void handleIncomingCommit() throws Exception {
    String correlationId = "correlationId";
    Map<Integer, Long> offsets = singletonMap(0, 0L);

    doReturn(true).when(consumer).commit(offsets);
    doReturn(true).when(metrics).record(eq(COMMIT), argThat(new ArgMatcher<Supplier<Boolean>>() {}));
    doNothing().when(underTest).sendEvent(any());

    underTest.handleIncomingEvent(new Commit(correlationId, offsets));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Supplier<Boolean>> captor = ArgumentCaptor.forClass(Supplier.class);
    verify(metrics).record(eq(COMMIT), captor.capture());
    assertThat(captor.getValue().get(), is(true));

    verify(metrics).markCommit(true);
    verify(underTest).sendEvent(new CommitResponse(correlationId, true));
  }

  @Test(expected = IllegalStateException.class)
  public void handleIncomingUnexpected() throws Exception {
    underTest.handleIncomingEvent(new Connection(""));
  }

  @Test
  public void replenishBuffer() throws Exception {
    Payload<JsonNode> payload = new Payload<JsonNode>((byte) 0, 1, mapper.createObjectNode());
    Record record = new Record(0, 1L, 3L, payload);
    List<Record> records = singletonList(record);

    doReturn(records).when(consumer).poll();
    doReturn(records).when(metrics).record(eq(POLL), argThat(new ArgMatcher<Supplier<Iterable<Record>>>() {}));

    underTest.replenishBuffer();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Supplier<Iterable<Record>>> recordsCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(metrics).record(eq(POLL), recordsCaptor.capture());
    assertThat(recordsCaptor.getValue().get(), is(records));

    ArgumentCaptor<Runnable> bufferCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(metrics).record(eq(BUFFER), bufferCaptor.capture());
    bufferCaptor.getValue().run();
    assertThat(underTest.getBuffer().peek(), is(record));
  }

  @Test
  public void sendMessage() throws Exception {
    JsonNode value = mapper.createObjectNode();
    Payload<JsonNode> payload = new Payload<JsonNode>((byte) 0, 2, value);
    Record record = new Record(0, 1L, 3L, payload);
    Message<JsonNode> message = new Message<>(0, 1L, 2, 3L, value);

    doReturn(value).when(messageFunction).apply(payload);
    doReturn(message).when(metrics).record(eq(MESSAGE), argThat(new ArgMatcher<Supplier<Message<JsonNode>>>() {}));
    doNothing().when(underTest).sendEvent(message);

    underTest.handleIncomingEvent(new Request(1L));
    underTest.sendMessage(record);

    verify(underTest).sendEvent(message);
    assertThat(underTest.getRequested(), is(0L));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Supplier<Message<JsonNode>>> captor = ArgumentCaptor.forClass(Supplier.class);
    verify(metrics).record(eq(MESSAGE), captor.capture());

    Message<JsonNode> message2 = captor.getValue().get();
    assertThat(message2, is(message));
  }

  @Test
  public void sendRebalance() throws Exception {
    Set<Integer> assignment = singleton(0);

    doNothing().when(underTest).sendEvent(any());

    underTest.sendRebalance(assignment);

    verify(underTest).sendEvent(new Rebalance(assignment));
  }

  @Test
  public void sendError() throws Exception {
    Exception exception = new IndexOutOfBoundsException("The reason is what it is!");

    doNothing().when(underTest).sendEvent(any());

    underTest.sendError(exception);

    verify(underTest).sendEvent(new Error("The reason is what it is!"));
  }

  @Test
  public void sendEvent() throws Exception {
    Event event = new Message<>(0, 1L, 2, 3L, mapper.createObjectNode());
    String raw = "";

    doReturn(raw).when(underTest).encodeEvent(event);

    underTest.sendEvent(event);

    ArgumentCaptor<Runnable> senderCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(metrics).record(eq(SEND), senderCaptor.capture());
    senderCaptor.getValue().run();
    verify(sender).send(raw);
  }

  @Test
  public void encodeMessage() throws Exception {
    Event event = new Message<>(0, 1L, 2, 3L, mapper.createObjectNode());
    String raw = "";

    doReturn(raw).when(encoder).encode(event);
    doReturn(raw).when(metrics).record(eq(ENCODE), argThat(new ArgMatcher<Supplier<String>>() {}));

    String result = underTest.encodeEvent(event);

    assertThat(result, is(raw));
    verify(metrics).markMessage(raw.length());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Supplier<String>> captor = ArgumentCaptor.forClass(Supplier.class);
    verify(metrics).record(eq(ENCODE), captor.capture());
    assertThat(captor.getValue().get(), is(raw));
  }

  @Test
  public void encodeNonMessage() throws Exception {
    Event event = new Rebalance(singleton(0));
    String raw = "";

    doReturn(raw).when(encoder).encode(event);
    doReturn(raw).when(metrics).record(eq(ENCODE), argThat(new ArgMatcher<Supplier<String>>() {}));

    String result = underTest.encodeEvent(event);

    assertThat(result, is(raw));
    verify(metrics, never()).markMessage(anyLong());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Supplier<String>> captor = ArgumentCaptor.forClass(Supplier.class);
    verify(metrics).record(eq(ENCODE), captor.capture());
    assertThat(captor.getValue().get(), is(raw));
  }

  static abstract class ArgMatcher<T> implements ArgumentMatcher<T> {
    @Override
    public boolean matches(T argument) {
      return true;
    }
  }
}
