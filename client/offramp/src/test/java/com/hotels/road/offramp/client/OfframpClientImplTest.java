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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.offramp.model.Cancel;
import com.hotels.road.offramp.model.CommitResponse;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.offramp.model.Rebalance;
import com.hotels.road.offramp.model.Request;

@RunWith(MockitoJUnitRunner.class)
public class OfframpClientImplTest {
  private @Mock WebSocket.Factory socketFactory;
  private @Mock WebSocket socket;
  private @Mock CommitHandler.Factory commitHandlerFactory;
  private @Mock CommitHandler commitHandler;
  private @Mock OfframpOptions<String> options;

  private final URI uri = URI.create("http://localhost");
  private final ObjectMapper mapper = new ObjectMapper();

  private OfframpClientImpl<String> underTest;

  @Before
  public void before() throws Exception {
    doReturn(uri).when(options).uri();
    doReturn("u").when(options).getUsername();
    doReturn("p").when(options).getPassword();
    doReturn(mapper).when(options).objectMapper();
    doReturn(10L).when(options).getKeepAliveSeconds();
    doReturn(socket).when(socketFactory).create(eq("u"), eq("p"), eq(uri), eq(mapper), any(), eq(10L), any(), any(),
        any());
    doReturn(commitHandler).when(commitHandlerFactory).create(any());
    underTest = new OfframpClientImpl<>(options, socketFactory, commitHandlerFactory);
  }

  @Test
  public void messages() throws Exception {
    doReturn(1).when(options).getInitialRequestAmount();
    doReturn(1).when(options).getReplenishingRequestAmount();
    Disposable subscribe = Flux.from(underTest.messages()).subscribe();
    subscribe.dispose();

    InOrder inOrder = Mockito.inOrder(socket);

    inOrder.verify(socket).send(new Request(1));
    inOrder.verify(socket).send(new Cancel());
  }

  @Test
  public void messagesRetry() throws Exception {
    doReturn(2).when(options).getInitialRequestAmount();
    doReturn(1).when(options).getReplenishingRequestAmount();
    doReturn(true).when(options).isRetry();

    Disposable subscribe = Flux.from(underTest.messages()).log().subscribe();
    Subscriber<Message<String>> value = underTest.getMessages();
    value.onNext(new Message<String>(0, 1L, 2, 3L, "foo"));
    value.onError(new Exception());
    Thread.sleep(1100); // TODO find a more efficient (read: faster) way of testing this
    value.onNext(new Message<String>(0, 1L, 2, 3L, "foo"));
    Thread.sleep(100);
    subscribe.dispose();

    InOrder inOrder = Mockito.inOrder(socket);

    inOrder.verify(socket).send(new Request(2));
    inOrder.verify(socket).send(new Request(1));
    inOrder.verify(socket).send(new Cancel());
  }

  @Test
  public void commit() throws Exception {
    doReturn(Mono.just(true)).when(commitHandler).commit(singletonMap(0, 1L));

    Boolean result = underTest.commit(singletonMap(0, 1L)).block();

    assertThat(result, is(true));
  }

  @Test
  public void rebalances() throws Exception {
    Set<Integer> assignment = singleton(0);
    Rebalance rebalance = new Rebalance(assignment);

    Disposable subscribe = Flux.from(underTest.rebalances()).limitRequest(1).subscribe();
    underTest.getRebalances().onNext(rebalance);
    subscribe.dispose();
  }

  @Test
  public void close() throws Exception {
    underTest.close();

    verify(socket).close();
  }

  @Test
  public void onNext_Message() throws Exception {
    doReturn(1).when(options).getInitialRequestAmount();
    doReturn(1).when(options).getReplenishingRequestAmount();
    AtomicReference<Message<String>> result = new AtomicReference<>();
    Flux.from(underTest.messages()).subscribe(result::set);
    Message<String> message = new Message<>(0, 1L, 2, 3L, "foo");
    underTest.onNext(message);
    await().pollInterval(100, MILLISECONDS).atMost(1, SECONDS).untilAtomic(result, is(message));
  }

  @Test
  public void onNext_CommitResponse() throws Exception {
    CommitResponse commitResponse = new CommitResponse("foo", true);
    underTest.onNext(commitResponse);
    verify(commitHandler).complete(commitResponse);
  }

  @Test
  public void onNext_Rebalance() throws Exception {
    AtomicReference<Rebalance> result = new AtomicReference<>();
    Flux.from(underTest.rebalances()).map(Rebalance::new).subscribe(result::set);
    Rebalance rebalance = new Rebalance(emptySet());
    underTest.onNext(rebalance);
    await().pollInterval(100, MILLISECONDS).atMost(1, SECONDS).untilAtomic(result, is(rebalance));
  }

  @Test
  public void onNext_Unknown() throws Exception {
    doReturn(1).when(options).getInitialRequestAmount();
    doReturn(1).when(options).getReplenishingRequestAmount();
    @SuppressWarnings("unchecked")
    Consumer<Throwable> error = mock(Consumer.class);
    Flux.from(underTest.messages()).doOnError(error).subscribe();
    underTest.onNext(new Cancel());
    await().pollDelay(100, MILLISECONDS).until(() -> verify(error, never()).accept(any()));
  }
}
