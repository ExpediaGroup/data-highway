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
package com.hotels.road.trafficcontrol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;
import com.hotels.road.trafficcontrol.model.KafkaRoad;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class UpdateModel {
  private final Map<String, KafkaRoad> store;
  private final PatchSetEmitter emitter;
  private final KafkaAdminClient adminClient;

  @Autowired
  public UpdateModel(@Value("#{store}") Map<String, KafkaRoad> store, PatchSetEmitter emitter, KafkaAdminClient adminClient) {
    this.store = store;
    this.emitter = emitter;
    this.adminClient = adminClient;
  }

  @Scheduled(initialDelayString = "${messageStatus.initialdelay:PT60m}", fixedRateString = "${messageStatus.fixedrate:PT60m}")
  public void updateMessageStatusInModel() {
    Map<String, KafkaRoad> store = new HashMap<>(this.store);
    store.forEach((key, model) -> {
      try {
        List<PatchOperation> operations = adminClient.updateMessageStatus(model);
        if (!operations.isEmpty()) {
          emitter.emit(new PatchSet(key, operations));
        }
      } catch (Exception e) {
        log.warn("Problem updating MessageStatus model \"{}\"", key, e);
      }
    });
  }
}
