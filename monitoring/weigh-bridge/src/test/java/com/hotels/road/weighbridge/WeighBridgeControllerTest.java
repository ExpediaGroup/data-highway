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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.road.weighbridge.model.Broker;

@RunWith(MockitoJUnitRunner.class)
public class WeighBridgeControllerTest {

  private WeighBridgeController underTest;

  @Test
  public void getCurrentBroker() {
    final Broker broker = new Broker(1, "rack", new ArrayList<>());
    final HashMap<Integer, Broker> map = new HashMap<>();
    underTest = new WeighBridgeController(new AtomicReference<>(broker), map);

    assertEquals(broker, underTest.getCurrentBroker());
  }

  @Test
  public void getBrokers() {
    final Broker broker1 = new Broker(1, "rack", new ArrayList<>());
    final Broker broker2 = new Broker(2, "rack", new ArrayList<>());
    final Broker broker3 = new Broker(3, "rack", new ArrayList<>());
    final HashMap<Integer, Broker> map = new HashMap<>();
    map.put(1, broker1);
    map.put(2, broker2);
    map.put(3, broker3);
    underTest = new WeighBridgeController(new AtomicReference<>(broker1), map);

    assertEquals(ImmutableList.of(broker1, broker2, broker3), underTest.getAllBrokers());
  }
}
