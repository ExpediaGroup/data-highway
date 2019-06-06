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
package com.hotels.road.client;

import static java.time.Duration.ofMillis;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Duration;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExponentialBackoffRetryHandlerTest {

  @Mock
  private Sleeper sleeper;

  @Test
  public void testMaxRetryCount() throws Exception  {
    ExponentialBackoffRetryHandler handler = new ExponentialBackoffRetryHandler(3, Duration.ZERO, Duration.ZERO, sleeper);
    doReturn(true).when(sleeper).sleep(any(Duration.class));
    assertThat(handler.retryRequest(mock(IOException.class), 3), is(true));
    assertThat(handler.retryRequest(mock(IOException.class), 4), is(false));
  }

  @Test
  public void testSleeping() throws Exception {
    ExponentialBackoffRetryHandler handler = new ExponentialBackoffRetryHandler(6, ofMillis(10), ofMillis(200), sleeper);
    handler.retryRequest(mock(IOException.class), 2);

    ArgumentCaptor<Duration> durationArgumentCaptor = ArgumentCaptor.forClass(Duration.class);
    verify(sleeper).sleep(durationArgumentCaptor.capture());
    assertThat(durationArgumentCaptor.getValue(), is(both(greaterThan(ofMillis(9L))).and(lessThan(ofMillis(31L)))));
  }

  @Test
  public void testSerializability() throws Exception  {
    ExponentialBackoffRetryHandler underTest = new ExponentialBackoffRetryHandler(3, Duration.ZERO, Duration.ZERO, sleeper);

    SerializationUtils.roundtrip(underTest);
  }

  @Test
  public void testSleepTimeCalculation() throws Exception {
    ExponentialBackoffRetryHandler handler = new ExponentialBackoffRetryHandler(3, ofMillis(10), ofMillis(200), sleeper);
    assertThat(handler.getSleepTime(1), is(ofMillis(10L))); //10 * (0 < x < 2)
    assertThat(handler.getSleepTime(2), is(both(greaterThan(ofMillis(9L))).and(lessThan(ofMillis(31L))))); //10 * (0 < x < 4)
    assertThat(handler.getSleepTime(3), is(both(greaterThan(ofMillis(9L))).and(lessThan(ofMillis(71L))))); //10 * (0 < x < 8)

    //capped by maxSleepTime
    assertThat(handler.getSleepTime(4), is(both(greaterThan(ofMillis(9L))).and(lessThan(ofMillis(151L))))); //10 * (0 < x < 16)
    assertThat(handler.getSleepTime(5), is(both(greaterThan(ofMillis(9L))).and(lessThan(ofMillis(201L))))); //10 * (0 < x < 32)
    assertThat(handler.getSleepTime(6), is(both(greaterThan(ofMillis(9L))).and(lessThan(ofMillis(201L))))); //10 * (0 < x < 64)
  }
}
