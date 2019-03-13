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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.UnknownDestinationException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.Destination;
import com.hotels.road.model.core.HiveDestination;
import com.hotels.road.model.core.Road;
import com.hotels.road.paver.api.HiveDestinationAdminClient;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

public class TollBoothHiveDestinationAdminClientTest {

  private static final String NAME = "road1";

  private final ObjectMapper mapper = new ObjectMapper();
  private final Map<String, Road> store = new HashMap<>();

  private final Road road = new Road();
  private final Map<String, Destination> destinations = new HashMap<>();
  private final HiveDestination hiveDestination = new HiveDestination();

  private HiveDestinationAdminClient underTest;

  @Before
  public void before() {
    PatchSetEmitter modificationEmitter = new PatchSetEmitter() {
      @Override
      public void emit(PatchSet roadPatch) {
        try {
          JsonNode roadJson = Optional
              .ofNullable(store.get(roadPatch.getDocumentId()))
              .map(r -> mapper.convertValue(r, JsonNode.class))
              .orElse(NullNode.instance);
          JsonNode patchJson = mapper.convertValue(roadPatch.getOperations(), JsonNode.class);
          JsonPatch jsonPatch = JsonPatch.fromJson(patchJson);
          JsonNode newRoadJson = jsonPatch.apply(roadJson);
          Road nnewRoad = mapper.convertValue(newRoadJson, Road.class);
          store.put(roadPatch.getDocumentId(), nnewRoad);
        } catch (IOException | JsonPatchException e) {
          throw new RuntimeException(e);
        }
      }
    };
    underTest = new TollBoothHiveDestinationAdminClient(store, modificationEmitter);

  }

  @Test
  public void getHiveDestination() throws Exception {
    destinations.put("hive", hiveDestination);
    road.setDestinations(destinations);
    store.put(NAME, road);

    Optional<HiveDestination> result = underTest.getHiveDestination(NAME);

    assertThat(result.isPresent(), is(true));
    assertThat(result.get(), is(hiveDestination));
  }

  @Test(expected = UnknownRoadException.class)
  public void getHiveDestination_UnknownRoad() throws Exception {
    underTest.getHiveDestination(NAME);
  }

  @Test
  public void getHiveDestination_NoDestination() throws Exception {
    store.put(NAME, road);

    Optional<HiveDestination> result = underTest.getHiveDestination(NAME);

    assertThat(result.isPresent(), is(false));
  }

  @Test
  public void createHiveDestination() throws Exception {
    road.setDestinations(destinations);
    store.put(NAME, road);

    underTest.createHiveDestination(NAME, hiveDestination);

    assertThat(store.get(NAME).getDestinations().get("hive"), is(hiveDestination));
  }

  @Test(expected = UnknownRoadException.class)
  public void createHiveDestination_UnknownRoad() throws Exception {
    underTest.createHiveDestination(NAME, hiveDestination);
  }

  @Test
  public void createHiveDestination_DestinationsNull() throws Exception {
    // handled in case it's null in the model
    store.put(NAME, road);

    underTest.createHiveDestination(NAME, hiveDestination);

    assertThat(store.get(NAME).getDestinations().get("hive"), is(hiveDestination));
  }

  @Test(expected = AlreadyExistsException.class)
  public void createHiveDestination_AlreadyExists() throws Exception {
    // handled in case it's null in the model
    destinations.put("hive", hiveDestination);
    road.setDestinations(destinations);
    store.put(NAME, road);

    underTest.createHiveDestination(NAME, hiveDestination);
  }

  @Test
  public void updateHiveDestination() throws Exception {
    destinations.put("hive", hiveDestination);
    road.setDestinations(destinations);
    store.put(NAME, road);

    HiveDestination updatedHiveDestination = new HiveDestination();
    updatedHiveDestination.setEnabled(true);

    underTest.updateHiveDestination(NAME, updatedHiveDestination);

    assertThat(store.get(NAME).getDestinations().get("hive"), is(updatedHiveDestination));
  }

  @Test(expected = UnknownRoadException.class)
  public void updateHiveDestination_UnknownRoad() throws Exception {
    underTest.updateHiveDestination(NAME, hiveDestination);
  }

  @Test(expected = UnknownDestinationException.class)
  public void updateHiveDestination_UnknownDestination() throws Exception {
    store.put(NAME, road);
    underTest.updateHiveDestination(NAME, hiveDestination);
  }

  @Test
  public void deleteHiveDestination() throws Exception {
    destinations.put("hive", hiveDestination);
    road.setDestinations(destinations);
    store.put(NAME, road);

    underTest.deleteHiveDestination(NAME);

    assertNull(store.get(NAME).getDestinations().get("hive"));
  }

  @Test(expected = UnknownRoadException.class)
  public void deleteHiveDestination_UnknownRoad() throws Exception {
    underTest.deleteHiveDestination(NAME);
  }

  @Test(expected = UnknownDestinationException.class)
  public void deleteHiveDestination_UnknownDestination() throws Exception {
    store.put(NAME, road);
    underTest.deleteHiveDestination(NAME);
  }

  @Test(expected = UnknownRoadException.class)
  public void deleteHiveDestination_deletedRoad() throws Exception {
    road.setDeleted(true);
    store.put(NAME, road);
    underTest.deleteHiveDestination(NAME);
  }

  @Test(expected = UnknownRoadException.class)
  public void updateHiveDestination_deletedRoad() throws Exception {
    road.setDeleted(true);
    store.put(NAME, road);
    underTest.updateHiveDestination(NAME,hiveDestination);
  }

  @Test(expected = UnknownRoadException.class)
  public void getHiveDestination_deletedRoad() throws Exception {
    road.setDeleted(true);
    store.put(NAME, road);
    underTest.getHiveDestination(NAME);
  }

  @Test(expected = UnknownRoadException.class)
  public void createHiveDestination_deletedRoad() throws Exception {
    road.setDeleted(true);
    store.put(NAME, road);
    underTest.createHiveDestination(NAME, hiveDestination);
  }
}
