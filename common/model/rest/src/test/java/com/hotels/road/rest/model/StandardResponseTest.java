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
package com.hotels.road.rest.model;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;

import org.junit.Test;

public class StandardResponseTest {
  private final long timestamp1 = 1L;
  private final boolean success = true;
  private final boolean failure = false;
  private final String message1 = "Aww yea!";
  private final String message2 = "Ohh noes!";

  @Test
  public void typical() {
    StandardResponse standardResponse = new StandardResponse(timestamp1, success, message1);

    assertThat(standardResponse.getTimestamp(), is(timestamp1));
    assertThat(standardResponse.isSuccess(), is(success));
    assertThat(standardResponse.getMessage(), is(message1));
  }

  @Test
  public void currentTimeConstructor() {
    Clock clock = mock(Clock.class);
    when(clock.millis()).thenReturn(timestamp1);

    StandardResponse standardResponse = new StandardResponse(clock, success, message1);
    assertThat(standardResponse.getTimestamp(), is(timestamp1));
  }

  @Test
  public void success() {
    StandardResponse standardResponse = StandardResponse.successResponse(message1);

    assertThat(standardResponse.isSuccess(), is(success));
    assertThat(standardResponse.getMessage(), is(message1));
  }

  @Test
  public void failure() {
    StandardResponse standardResponse = StandardResponse.failureResponse(message2);

    assertThat(standardResponse.isSuccess(), is(failure));
    assertThat(standardResponse.getMessage(), is(message2));
  }
}
