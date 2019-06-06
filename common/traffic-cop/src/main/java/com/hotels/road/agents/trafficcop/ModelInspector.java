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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@ConditionalOnBean(Agent.class)
@Component
@Slf4j
@RequiredArgsConstructor
public class ModelInspector<M> {
  private final Map<String, M> store;
  private final Agent<M> agent;
  private final PatchSetEmitter emitter;

  @Scheduled(initialDelayString = "${model.inspection.interval:60000}", fixedRateString = "${model.inspection.interval:60000}")
  public void inspect() {
    Map<String, M> store = new HashMap<>(this.store);
    store.forEach((key, model) -> {
      // TODO: Timing metrics
      try {
        List<PatchOperation> operations = agent.inspectModel(key, model);
        if (!operations.isEmpty()) {
          emitter.emit(new PatchSet(key, operations));
        }
      } catch (Exception e) {
        log.warn("Problem inspecting model \"{}\"", key, e);
        // TODO: count metrics
      }
    });
  }
}
