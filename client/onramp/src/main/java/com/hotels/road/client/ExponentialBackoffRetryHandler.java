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

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ExponentialBackoffRetryHandler implements RetryHandler {
  private final Random random = new Random();

  private final int maxRetries;
  private final Duration baseBackOffDuration;
  private final Duration maxBackOffDuration;
  private final Sleeper sleeper;

  @Override
  public boolean retryRequest(IOException exception, int executionCount) {
    if (executionCount <= maxRetries) {
      Duration sleepTime = getSleepTime(executionCount);

      return sleeper.sleep(sleepTime);
    } else {
      return false;
    }
  }

  Duration getSleepTime(int executionCount) {
    //( 1 << 31 = -2147483648 ), So
    executionCount = executionCount > 30 ? 30 : executionCount;
    Duration sleepTime = baseBackOffDuration.multipliedBy(Math.max(1, random.nextInt(1 << executionCount)));
    return sleepTime.compareTo(maxBackOffDuration) > 0 ? maxBackOffDuration : sleepTime;
  }
}
