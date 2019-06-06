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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import reactor.core.publisher.Flux;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.hotels.road.offramp.client.OfframpClient;
import com.hotels.road.offramp.model.Message;

@RunWith(MockitoJUnitRunner.class)
public class ReceiverTest {
  private final String origin = "fake";
  private @Mock MessagesMetricSet metrics;
  private @Mock TestMessageContextManager contextManager;
  private @Mock OfframpClient<TestMessage> offramp;

  private final TestMessage message1 = new TestMessage(origin, 0, 1L, 1L, "message1");
  private final TestMessage message2 = new TestMessage(origin, 1, 2L, 2L, "message2");

  private @Mock TestMessageContext context1;
  private @Mock TestMessageContext context2;

  @Before
  public void before() throws Exception {
    when(contextManager.getContext(message1)).thenReturn(context1);
    when(contextManager.getContext(message2)).thenReturn(context2);

    when(offramp.messages()).thenReturn(Flux.<Message<TestMessage>> fromIterable(ImmutableList
        .<Message<TestMessage>> builder()
        .add(new Message<>(0, 1, 1, 2L, message1))
        .add(new Message<>(0, 2, 1, 3L, message2))
        .build()));

    when(offramp.rebalances()).thenReturn(Flux.<Set<Integer>> fromIterable(ImmutableList
        .<Set<Integer>> builder()
        .add(Sets.<Integer> newHashSet())
        .add(Sets.<Integer> newHashSet(0, 1, 2, 3, 4, 5))
        .build()));
  }

  @Test
  public void worker_passes_to_context() throws Exception {
    try (Receiver receiver = new Receiver(metrics, contextManager, origin, offramp)) {
      receiver.start();

      verify(context1).messageReceived(message1);
      verify(context2).messageReceived(message2);
    }
  }
}
