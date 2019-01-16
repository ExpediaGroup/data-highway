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

import java.io.IOException;

import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;

import com.hotels.road.client.RetryHandler;

class RetryHandlerWrapper extends DefaultHttpRequestRetryHandler {

  private final RetryHandler retryHandler;

  RetryHandlerWrapper(RetryHandler retryHandler) {
    super(Integer.MAX_VALUE, false);
    this.retryHandler = retryHandler;
  }

  @Override
  public boolean retryRequest(
      final IOException exception,
      final int executionCount,
      final HttpContext context) {
    return super.retryRequest(exception, executionCount, context)
        && retryHandler.retryRequest(exception, executionCount);
  }
}
