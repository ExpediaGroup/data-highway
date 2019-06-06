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
package com.hotels.road.testdrive;

import static java.lang.String.format;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.collect.ImmutableSortedSet;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.KafkaStatus;
import com.hotels.road.model.core.Road;
import com.hotels.road.paver.api.RoadAdminClient;
import com.hotels.road.tollbooth.client.api.PatchSet;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
class MemoryRoadAdminClient implements RoadAdminClient {
  private static final String ROAD_ALREADY_EXISTS = "Road %s already exists.";
  private static final String DELETED_ROAD_ALREADY_EXISTS = "Road %s is currently being deleted. Please try again later.";
  private final Map<String, Road> store;
  private final ObjectMapper mapper;

  @Override
  public SortedSet<String> listRoads() {
    return ImmutableSortedSet.copyOf(store.keySet());
  }

  @Override
  public Optional<Road> getRoad(String name) {
    checkNotBlank(name, "name");
    return Optional.ofNullable(store.get(name));
  }

  @Override
  public void createRoad(Road road) throws AlreadyExistsException {
    getRoad(road.getName()).ifPresent(r -> {
      if(r.isDeleted()) {
        throw new AlreadyExistsException(String.format(DELETED_ROAD_ALREADY_EXISTS, r.getName()));
      }
      throw new AlreadyExistsException(String.format(ROAD_ALREADY_EXISTS, r.getName()));
    });
    KafkaStatus status = new KafkaStatus();
    status.setTopicCreated(true);
    road.setStatus(status);
    store.put(road.getName(), road);
  }

  @Override
  public void updateRoad(PatchSet patch) throws UnknownRoadException, IllegalArgumentException, ServiceException {
    String roadName = patch.getDocumentId();
    Road road = getRoad(roadName)
        .map(r -> mapper.convertValue(r, JsonNode.class))
        .map(r -> applyPatch(r, patch))
        .map(r -> mapper.convertValue(r, Road.class))
        .orElseThrow(() -> new UnknownRoadException(roadName));
    store.put(roadName, road);
  }

  JsonNode applyPatch(JsonNode road, PatchSet patch) {
    try {
      JsonNode jsonNodePatch = mapper.convertValue(patch.getOperations(), JsonNode.class);
      JsonPatch jsonPatch = JsonPatch.fromJson(jsonNodePatch);
      return jsonPatch.apply(road);
    } catch (IOException | JsonPatchException e) {
      throw new ServiceException(e);
    }
  }

  private static void checkNotBlank(String value, String attribute) {
    checkArgument(isNotBlank(value), format("Road %s must not be blank.", attribute));
  }
}
