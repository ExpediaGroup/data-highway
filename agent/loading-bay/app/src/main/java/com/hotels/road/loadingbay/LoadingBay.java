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
package com.hotels.road.loadingbay;

import static java.util.Collections.emptyList;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.loadingbay.model.Destinations;
import com.hotels.road.loadingbay.model.Hive;
import com.hotels.road.loadingbay.model.HiveRoad;
import com.hotels.road.loadingbay.model.HiveStatus;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@Slf4j
@Component
public class LoadingBay implements Agent<HiveRoad> {

  static final OffsetDateTime EPOCH = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
  private final HiveTableAction hiveTableAction;
  private final Function<HiveRoad, LanderMonitor> monitorFactory;
  private final Map<String, LanderMonitor> monitors;

  @Autowired
  public LoadingBay(HiveTableAction hiveTableAction, Function<HiveRoad, LanderMonitor> monitorFactory) {
    this.hiveTableAction = hiveTableAction;
    this.monitorFactory = monitorFactory;

    monitors = new HashMap<>();
  }

  @Override
  public List<PatchOperation> newModel(String key, HiveRoad newModel) {
    return inspectModel(key, newModel);
  }

  @Override
  public List<PatchOperation> updatedModel(String key, HiveRoad oldModel, HiveRoad newModel) {
    return Collections.emptyList();
  }

  @Override
  public void deletedModel(String key, HiveRoad oldModel) {
    log.warn("I don't know how to handle model deletion.");
  }

  @Override
  public List<PatchOperation> inspectModel(String key, HiveRoad model) {
    Optional<Hive> hive = Optional.ofNullable(model.getDestinations()).map(Destinations::getHive);
    if (hive.isPresent()) {
      OffsetDateTime lastRun = hive.map(Hive::getStatus).map(HiveStatus::getLastRun).orElse(EPOCH);
      log.debug("Inspecting road {}. Lander last ran at {}", model.getName(), lastRun);
      try {
        List<PatchOperation> patches = hiveTableAction.checkAndApply(model);
        LanderMonitor monitor = monitors.computeIfAbsent(model.getName(), n -> monitorFactory.apply(model));
        monitor.establishLandingFrequency(hive.map(Hive::getLandingInterval).orElse(Hive.DEFAULT_LANDING_INTERVAL));
        monitor.setEnabled(hive.get().isEnabled());
        return patches;
      } catch (NoActiveSchemaException e) {
        log.info("No schema defined on road '{}'", model.getName());
        return emptyList();
      } catch (Exception e) {
        log.error("Error while applying actions for road '{}'", model.getName(), e);
        return emptyList();
      }
    } else {
      log.info("Skipping road {} because it has no Hive destination", model.getName());
      if (monitors.containsKey(model.getName())) {
        try {
          monitors.remove(model.getName()).close();
        } catch (Exception e) {
          log.warn("Error shutting down DestinationMonitor for {}", model.getName(), e);
        }
      }
      return emptyList();
    }
  }
}
