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

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSortedSet;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.Road;
import com.hotels.road.paver.api.RoadAdminClient;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@Component
public class TollboothRoadAdminClient implements RoadAdminClient {
  private static final String ROAD_ALREADY_EXISTS = "Road %s already exists.";
  private final Map<String, Road> store;
  private final PatchSetEmitter modificationEmitter;

  @Autowired
  public TollboothRoadAdminClient(@Value("#{store}") Map<String, Road> store, PatchSetEmitter modificationEmitter) {
    this.store = store;
    this.modificationEmitter = modificationEmitter;
  }

  @Override
  public SortedSet<String> listRoads() throws ServiceException {
    return ImmutableSortedSet.copyOf(store.entrySet().stream().filter(r -> !r.getValue().isDeleted())
      .map(x -> x.getKey()).collect(Collectors.toSet()));
  }

  @Override
  public Optional<Road> getRoad(String name) throws IllegalArgumentException, ServiceException {
    checkNotBlank(name, "name");
    Road road = store.get(name);
    if(road == null || road.isDeleted()) {
      return Optional.empty();
    }
    return Optional.ofNullable(road);
  }

  @Override
  public void createRoad(Road road) throws AlreadyExistsException, IllegalArgumentException, ServiceException {
    getRoad(road.getName()).ifPresent(r -> {
      throw new AlreadyExistsException(String.format(ROAD_ALREADY_EXISTS, r.getName()));
    });

    modificationEmitter.emit(new PatchSet(road.getName(), singletonList(PatchOperation.add("", road))));
  }

  @Override
  public void updateRoad(PatchSet patchSet) throws UnknownRoadException, IllegalArgumentException, ServiceException {
    modificationEmitter.emit(patchSet);
  }

  private static void checkNotBlank(String value, String attribute) {
    checkArgument(isNotBlank(value), format("Road %s must not be blank.", attribute));
  }
}
