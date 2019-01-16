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
package com.hotels.road.agents.trafficcop;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.kafkastore.StoreUpdateObserver;

@ConditionalOnMissingBean(Agent.class)
@Component
public class NullStoreUpdateObserver<M> implements StoreUpdateObserver<String, M> {
  @Override
  public void handleNew(String key, M value) {}

  @Override
  public void handleUpdate(String key, M oldValue, M newValue) {}

  @Override
  public void handleRemove(String key, M oldValue) {}
}
