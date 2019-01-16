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
package com.hotels.road.offramp.service;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.hotels.road.offramp.metrics.OfframpMetrics;
import com.hotels.road.offramp.socket.EventSender;
import com.hotels.road.offramp.spi.RoadConsumer;

@Component
@RequiredArgsConstructor
public class OfframpServiceFactory {
  private final OfframpServiceV2.Factory serviceV2Factory;

  public OfframpService create(
      String version,
      RoadConsumer consumer,
      MessageFunction messageFunction,
      EventSender sender,
      OfframpMetrics metrics)
    throws Exception {
    switch (OfframpVersion.fromString(version)) {
    case OFFRAMP_2:
      return serviceV2Factory.create(consumer, messageFunction, sender, metrics);
    default:
      throw new Exception("Unknown OfframpVersion " + version);
    }
  }
}
