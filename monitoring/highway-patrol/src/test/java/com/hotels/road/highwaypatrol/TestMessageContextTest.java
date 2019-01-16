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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.rest.model.StandardResponse;

@RunWith(MockitoJUnitRunner.class)
public class TestMessageContextTest {
  private final TestMessage message = new TestMessage("fake", 0, 1L, 1L, "message1");
  private @Mock MessagesMetricSet metrics;
  private TestMessageContext context;

  @Before
  public void before() throws Exception {
    context = new TestMessageContext(message, Duration.ofSeconds(10), metrics, null) {
      @Override
      protected void addAction(Runnable action) {
        action.run();
      }
    };
  }

  @Test
  public void successful_send_is_logged() throws Exception {
    context.messageSent(CompletableFuture.completedFuture(new StandardResponse(1L, true, "")));
    assertThat(context.getState(), is(MessageState.SEND_SUCCESS));
  }

  @Test
  public void send_exception_is_logged() throws Exception {
    CompletableFuture<StandardResponse> response = new CompletableFuture<>();
    response.completeExceptionally(new IOException());
    context.messageSent(response);
    assertThat(context.getState(), is(MessageState.SEND_FAILURE));
  }

  @Test
  public void rejected_send_is_logged() throws Exception {
    context.messageSent(CompletableFuture.completedFuture(new StandardResponse(1L, false, "")));
    assertThat(context.getState(), is(MessageState.SEND_REJECTED));
  }

  @Test
  public void successful_receive_is_logged() throws Exception {
    /* TestMessageContext previous = */new TestMessageContext(message, Duration.ofSeconds(10), metrics, null);
    context.messageReceived(message);
    assertThat(context.getState(), is(MessageState.RECEIVED_CORRECT));
  }

  @Test
  public void successful_receive_after_previous_is_out_of_order() throws Exception {
    TestMessageContext previous = new TestMessageContext(message, Duration.ofSeconds(10), metrics, null);
    previous.setReceived(false);
    previous.setState(MessageState.SEND_SUCCESS);
    context.setPrevious(previous);
    context.messageReceived(message);
    assertThat(context.getState(), is(MessageState.RECEIVED_OUT_OF_ORDER));
  }

  @Test
  public void successful_receive_after_unsuccessful_send_is_correct() throws Exception {
    TestMessageContext previous = new TestMessageContext(message, Duration.ofSeconds(10), metrics, null);
    previous.setReceived(false);
    previous.setState(MessageState.SEND_FAILURE);
    context.setPrevious(previous);
    context.messageReceived(message);
    assertThat(context.getState(), is(MessageState.RECEIVED_CORRECT));
  }
}
