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
package com.hotels.road.agents.trafficcop;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.kafkastore.StoreUpdateObserver;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@ConditionalOnBean(Agent.class)
@Component
@RequiredArgsConstructor
public class AgentStoreObserver<M> implements StoreUpdateObserver<String, M> {
  private final Agent<M> agent;
  private final PatchSetEmitter emitter;

  @Override
  public void handleNew(String key, M value) {
    List<PatchOperation> operations = agent.newModel(key, value);
    if (!operations.isEmpty()) {
      emitter.emit(new PatchSet(key, operations));
    }
  }

  @Override
  public void handleUpdate(String key, M oldValue, M newValue) {
    if (!oldValue.equals(newValue)) {
      List<PatchOperation> operations = agent.updatedModel(key, oldValue, newValue);
      if (!operations.isEmpty()) {
        emitter.emit(new PatchSet(key, operations));
      }
    }
  }

  @Override
  public void handleRemove(String key, M oldValue) {
    agent.deletedModel(key, oldValue);
  }
}
