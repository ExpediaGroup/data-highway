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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;

import com.hotels.road.offramp.model.Cancel;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.offramp.model.Request;

@RunWith(MockitoJUnitRunner.class)
public class MessageProcessorTest {
  private @Mock EventSender eventSender;
  private @Mock Subscriber<? super Message<String>> subscriber;

  private MessageProcessor<String> underTest;

  @Before
  public void before() {
    underTest = new MessageProcessor<>(eventSender);
  }

  @Test
  public void subscribe() throws Exception {
    underTest.subscribe(subscriber);

    verify(subscriber).onSubscribe(underTest);
  }

  @Test
  public void request() throws Exception {
    underTest.request(1);

    verify(eventSender).send(new Request(1));
  }

  @Test
  public void cancel() throws Exception {
    underTest.cancel();

    verify(eventSender).send(new Cancel());
  }

  @Test
  public void onNext() throws Exception {
    Message<String> message = new Message<>(0, 1L, 2, 3L, "foo");

    underTest.subscribe(subscriber);
    underTest.onNext(message);

    verify(subscriber).onNext(message);
  }

  @Test
  public void onError() throws Exception {
    Exception exception = new Exception();

    underTest.subscribe(subscriber);
    underTest.onError(exception);
    underTest.onError(exception);

    verify(subscriber, times(1)).onError(exception);
  }
}
