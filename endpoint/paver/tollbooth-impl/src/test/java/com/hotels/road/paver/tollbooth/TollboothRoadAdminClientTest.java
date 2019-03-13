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

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.model.core.KafkaStatus;
import com.hotels.road.model.core.Road;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

public class TollboothRoadAdminClientTest {
  private final ObjectMapper mapper = new ObjectMapper();

  private TollboothRoadAdminClient client;
  private Map<String, Road> store;

  private Road road1;
  private KafkaStatus status;

  @Before
  public void before() {
    road1 = new Road();
    road1.setName("road1");
    road1.setTopicName("road1");
    road1.setDescription("description");
    road1.setContactEmail("contactEmail");
    road1.setEnabled(false);
    status = new KafkaStatus();
    status.setTopicCreated(false);
    road1.setStatus(status);
    road1.setDeleted(false);

    PatchSetEmitter patchSetEmitter = new PatchSetEmitter() {

      @Override
      public void emit(PatchSet patchSet) {
        try {
          JsonNode roadJson = Optional
              .ofNullable(store.get(patchSet.getDocumentId()))
              .map(r -> mapper.convertValue(r, JsonNode.class))
              .orElse(NullNode.instance);
          JsonNode patchJson = mapper.convertValue(patchSet.getOperations(), JsonNode.class);
          JsonPatch jsonPatch = JsonPatch.fromJson(patchJson);
          JsonNode newRoadJson = jsonPatch.apply(roadJson);
          Road nnewRoad = mapper.convertValue(newRoadJson, Road.class);
          store.put(patchSet.getDocumentId(), nnewRoad);
        } catch (IOException | JsonPatchException e) {
          throw new RuntimeException(e);
        }
      }
    };

    store = new HashMap<>();
    client = new TollboothRoadAdminClient(Collections.unmodifiableMap(store), patchSetEmitter);
  }

  @Test
  public void listRoads() throws Exception {
    Set<String> roads;
    roads = client.listRoads();
    assertTrue(roads.isEmpty());

    store.put("road1", road1);

    roads = client.listRoads();
    assertThat(roads.size(), is(1));
    assertTrue(roads.contains("road1"));
  }

  @Test
  public void listRoads_deleted() throws Exception {
    road1.setDeleted(true);
    store.put("road1", road1);
    Set<String> roads;
    roads = client.listRoads();
    assertTrue(roads.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getRoad_fails_when_blank() throws Exception {
    client.getRoad(" ");
  }

  @Test
  public void getRoad_returns_empty_optional_when_not_present() throws Exception {
    Optional<Road> road = client.getRoad("no_road");
    assertFalse(road.isPresent());
  }

  @Test
  public void getRoad_returns_present_road() throws Exception {
    store.put(road1.getName(), road1);
    Optional<Road> road = client.getRoad(road1.getName());
    assertTrue(road.isPresent());
    assertThat(road.get(), is(road1));
  }

  @Test
  public void getRoad_returns_empty_optional_when_road_isDeleted() throws Exception {
    road1.setDeleted(true);
    store.put(road1.getName(), road1);
    Optional<Road> road = client.getRoad(road1.getName());
    assertFalse(road.isPresent());
  }

  @Test
  public void createRoad() throws Exception {
    client.createRoad(road1);

    assertTrue(store.containsKey(road1.getName()));
    assertThat(store.get(road1.getName()), is(road1));
  }

  @Test(expected = AlreadyExistsException.class)
  public void createRoadThatAlreadyExist() throws Exception {
    client.createRoad(road1);
    client.createRoad(road1);
  }

  @Test
  public void updateRoad() throws Exception {
    store.put(road1.getName(), road1);
    client.updateRoad(new PatchSet(road1.getName(), singletonList(PatchOperation.replace("/enabled", Boolean.TRUE))));

    assertTrue(store.get(road1.getName()).isEnabled());
  }

  @Test
  public void updateRoad_withDeleteRoadPatch() throws Exception {
    store.put(road1.getName(), road1);
    client.updateRoad(new PatchSet(road1.getName(), singletonList(PatchOperation.remove(""))));
    assertNull(store.get(road1.getName()));
  }
}
