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
package com.hotels.road.paver.tollbooth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.UnknownDestinationException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.Destination;
import com.hotels.road.model.core.HiveDestination;
import com.hotels.road.model.core.Road;
import com.hotels.road.paver.api.HiveDestinationAdminClient;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@Component
public class TollBoothHiveDestinationAdminClient implements HiveDestinationAdminClient {

  private static final String DESTINATIONS = "/destinations";
  private static final String DESTINATIONS_HIVE = "/destinations/hive";

  private final Map<String, Road> roads;
  private final PatchSetEmitter modificationEmitter;

  @Autowired
  public TollBoothHiveDestinationAdminClient(
      @Value("#{store}") Map<String, Road> roads,
      PatchSetEmitter modificationEmitter) {
    this.roads = roads;
    this.modificationEmitter = modificationEmitter;
  }

  @Override
  public Optional<HiveDestination> getHiveDestination(String name) throws UnknownRoadException {
    return destinations(name).map(e -> (HiveDestination) e.get("hive"));
  }

  @Override
  public void createHiveDestination(String name, HiveDestination hiveDestination)
    throws UnknownRoadException, AlreadyExistsException {
    if (getHiveDestination(name).isPresent()) {
      throw new AlreadyExistsException(String.format("Hive destination for Road \"%s\" already exists.", name));
    }

    List<PatchOperation> operations = new ArrayList<>();

    if (!destinations(name).isPresent()) {
      operations.add(PatchOperation.add(DESTINATIONS, new HashMap<>()));
    }

    operations.add(PatchOperation.add(DESTINATIONS_HIVE, hiveDestination));
    PatchSet patchSet = new PatchSet(name, operations);
    modificationEmitter.emit(patchSet);
  }

  @Override
  public void deleteHiveDestination(String name)
      throws UnknownRoadException, UnknownDestinationException {
    getHiveDestination(name).orElseThrow(() -> new UnknownDestinationException("Hive", name));

    List<PatchOperation> operations = new ArrayList<>();
    operations.add(PatchOperation.remove(DESTINATIONS_HIVE));
    PatchSet patchSet = new PatchSet(name, operations);
    modificationEmitter.emit(patchSet);
  }

  @Override
  public void updateHiveDestination(String name, HiveDestination hiveDestination)
    throws UnknownRoadException, UnknownDestinationException {
    getHiveDestination(name).orElseThrow(() -> new UnknownDestinationException("Hive", name));

    List<PatchOperation> operations = Collections
        .singletonList(PatchOperation.replace(DESTINATIONS_HIVE, hiveDestination));
    PatchSet patchSet = new PatchSet(name, operations);
    modificationEmitter.emit(patchSet);
  }

  private Road road(String name) throws UnknownRoadException {
    return Optional.ofNullable(roads.get(name)).orElseThrow(() -> new UnknownRoadException(name));
  }

  private Optional<Map<String, Destination>> destinations(String name) throws UnknownRoadException {
    return Optional.of(road(name)).map(Road::getDestinations);
  }

}
