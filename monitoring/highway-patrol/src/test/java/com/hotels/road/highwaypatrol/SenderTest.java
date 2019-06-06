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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.client.AsyncRoadClient;
import com.hotels.road.rest.model.StandardResponse;

@RunWith(MockitoJUnitRunner.class)
public class SenderTest {
  private @Mock AsyncRoadClient<TestMessage> onrampClient;
  private @Mock TestMessageContextManager contextManager;
  private @Mock ScheduledExecutorService service;

  private @Captor ArgumentCaptor<TimeUnit> timeunitCaptor;
  private @Captor ArgumentCaptor<Long> longCaptor;
  private @Captor ArgumentCaptor<Runnable> runnableCaptor;

  @Before
  public void before() throws Exception {
    when(service.awaitTermination(anyLong(), any())).thenReturn(true);
  }

  @Test
  @SuppressWarnings("resource")
  public void _250Hz_correctly_sets_4ms_job_interval() throws Exception {
    new Sender(onrampClient, contextManager, 250, service).start();
    verify(service).scheduleAtFixedRate(any(), eq(0L), longCaptor.capture(), timeunitCaptor.capture());
    assertThat(timeunitCaptor.getValue().toMillis(longCaptor.getValue()), is(4L));
  }

  @SuppressWarnings("resource")
  @Test(expected = IllegalArgumentException.class)
  public void _0Hz_causes_error() throws Exception {
    new Sender(onrampClient, contextManager, 0, service);
  }

  @SuppressWarnings("resource")
  @Test(expected = IllegalArgumentException.class)
  public void hz_must_divide_1000() throws Exception {
    new Sender(onrampClient, contextManager, 3, service);
  }

  @Test
  public void close_shuts_down_service() throws Exception {
    new Sender(onrampClient, contextManager, 1L, service).close();
    verify(service).shutdown();
  }

  @Test(expected = Exception.class)
  public void close_throws_if_service_doesnt_shutdown() throws Exception {
    when(service.awaitTermination(anyLong(), any())).thenReturn(false);
    new Sender(onrampClient, contextManager, 1L, service).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void worker_correctly_gets_and_sends_message() throws Exception {
    try (Sender sender = new Sender(onrampClient, contextManager, 1L, service)) {
      sender.start();
      verify(service).scheduleAtFixedRate(runnableCaptor.capture(), anyLong(), anyLong(), any());

      TestMessageContext context = mock(TestMessageContext.class);
      TestMessage message = mock(TestMessage.class);
      CompletableFuture<StandardResponse> future = mock(CompletableFuture.class);

      when(context.getMessage()).thenReturn(message);
      when(contextManager.nextContext()).thenReturn(context);
      when(onrampClient.sendMessage(eq(message))).thenReturn(future);

      runnableCaptor.getValue().run();

      verify(contextManager).nextContext();
      verify(onrampClient).sendMessage(message);
      verify(context).messageSent(future);
    }
  }
}
