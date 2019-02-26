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
package com.hotels.road.weighbridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hotels.road.weighbridge.model.Broker;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class WeighBridgeController {

  private final AtomicReference<Broker> brokerReference;
  private final Map<Integer, Broker> map;

  @GetMapping("/brokers/current")
  public Broker getCurrentBroker() {
    return brokerReference.get();
  }

  @GetMapping("/brokers")
  public List<Broker> getAllBrokers() {
    return new ArrayList<>(map.values());
  }
}
