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
package com.hotels.road.client;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

/**
 * A handler for determining if a Send Request should be retried after a
 * recoverable exception during execution.
 * <p>
 * Implementations of this interface must be thread-safe. Access to shared
 * data must be synchronized as methods of this interface may be executed
 * from multiple threads.
 */
public interface RetryHandler extends Serializable {
  /**
   * Determines if a method should be retried after an IOException
   * occurs during execution.
   *
   * @param exception      the exception that occurred
   * @param executionCount the number of times this method has been
   *                       unsuccessfully executed
   * @return {@code true} if the method should be retried, {@code false}
   * otherwise
   */
  boolean retryRequest(IOException exception, int executionCount);

  static RetryHandler retryOnce() {
    return retryNTimes(1);
  }

  static RetryHandler retryNTimes(int maxRetries) {
    return retryNTimesBackingOffExponentially(maxRetries, Duration.ZERO, Duration.ZERO);
  }

  static RetryHandler retryNTimesBackingOffExponentially(int maxRetries, Duration baseBackOffDuration,
                                                         Duration maxBackOffDuration) {
    return new ExponentialBackoffRetryHandler(maxRetries, baseBackOffDuration, maxBackOffDuration, new ThreadSleeper());
  }
}


