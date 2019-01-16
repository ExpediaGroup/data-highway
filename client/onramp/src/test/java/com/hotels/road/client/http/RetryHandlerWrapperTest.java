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
package com.hotels.road.client.http;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import javax.net.ssl.SSLException;

import org.apache.http.protocol.HttpContext;
import org.junit.Test;

import com.hotels.road.client.RetryHandler;

public class RetryHandlerWrapperTest {
  @Test
  public void testForNonRetriableExceptions() throws Exception {
    RetryHandlerWrapper handler = new RetryHandlerWrapper(mock(RetryHandler.class));
    assertThat(handler.retryRequest(mock(InterruptedIOException.class), 1, mock(HttpContext.class)), is(false));
    assertThat(handler.retryRequest(mock(UnknownHostException.class), 1, mock(HttpContext.class)), is(false));
    assertThat(handler.retryRequest(mock(ConnectException.class), 1, mock(HttpContext.class)), is(false));
    assertThat(handler.retryRequest(mock(SSLException.class), 1, mock(HttpContext.class)), is(false));
  }

  @Test
  public void testForRetriableExceptions() throws Exception {
    RetryHandler retryHandler = mock(RetryHandler.class);
    RetryHandlerWrapper handler = new RetryHandlerWrapper(retryHandler);
    doReturn(true).when(retryHandler).retryRequest(any(IOException.class), anyInt());

    assertThat(handler.retryRequest(mock(IOException.class), 29, mock(HttpContext.class)), is(true));

    verify(retryHandler).retryRequest(any(IOException.class), eq(29));
  }
}
