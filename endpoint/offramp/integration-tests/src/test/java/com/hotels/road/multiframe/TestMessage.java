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
package com.hotels.road.multiframe;

import static java.lang.String.join;
import static java.util.Collections.nCopies;

import com.hotels.road.offramp.model.Message;

import lombok.Getter;

public class TestMessage {

  @Getter
  private static String payload = join("", nCopies(1024, "x"));

  public static Message<String> getTestMessage()
  {
    return new Message<>(0, 1L, 2, 3L, payload);
  }

}
