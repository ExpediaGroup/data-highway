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
package com.hotels.road.tollbooth.app;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import static com.hotels.road.tollbooth.client.api.Operation.ADD;
import static com.hotels.road.tollbooth.client.api.Operation.REMOVE;
import static com.hotels.road.tollbooth.client.api.Operation.REPLACE;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;

import com.hotels.road.tollbooth.client.api.Operation;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;

public class PatchProcessorTest {

  private static final String DOCUMENT_ID = "documentId";

  private final Map<String, JsonNode> store = new HashMap<>();
  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonPatchApplier patchApplier = new JsonPatchApplier(mapper);

  private final Map<String, String> map1 = singletonMap("foo", "bar");
  private final Map<String, String> map2 = singletonMap("foo", "baz");
  private final JsonNode jsonNode1 = mapper.createObjectNode().put("foo", "bar");
  private final JsonNode jsonNode2 = mapper.createObjectNode().put("foo", "baz");

  private final PatchProcessor underTest = new PatchProcessor(store, patchApplier);

  @Test
  public void addNew() throws PatchApplicationException {
    PatchSet patchSet = patchSet(ADD, "", map1);

    JsonNode result = underTest.processPatch(patchSet);

    assertThat(result, is(jsonNode1));
    assertThat(store.get(DOCUMENT_ID), is(jsonNode1));
  }

  @Test
  public void addExisting() throws PatchApplicationException {
    store.put(DOCUMENT_ID, jsonNode1);
    PatchSet patchSet = patchSet(ADD, "", map2);

    JsonNode result = underTest.processPatch(patchSet);

    assertThat(result, is(jsonNode2));
    assertThat(store.get(DOCUMENT_ID), is(jsonNode2));
  }

  @Test
  public void replaceNew() throws PatchApplicationException {
    PatchSet patchSet = patchSet(REPLACE, "", map1);

    JsonNode result = underTest.processPatch(patchSet);

    assertThat(result, is(jsonNode1));
    assertThat(store.get(DOCUMENT_ID), is(jsonNode1));
  }

  @Test
  public void replaceExisting() throws PatchApplicationException {
    store.put(DOCUMENT_ID, jsonNode1);
    PatchSet patchSet = patchSet(REPLACE, "", singletonMap("foo", "baz"));

    JsonNode result = underTest.processPatch(patchSet);

    assertThat(result, is(jsonNode2));
    assertThat(store.get(DOCUMENT_ID), is(jsonNode2));
  }

  @Test
  public void removeNonExistingRoot() throws PatchApplicationException {
    PatchSet patchSet = patchSet(REMOVE, "", null);

    JsonNode result = underTest.processPatch(patchSet);

    assertThat(result, is(MissingNode.getInstance()));
    assertThat(store.get(DOCUMENT_ID), is(nullValue()));
  }

  @Test
  public void removeExistingRoot() throws PatchApplicationException {
    store.put(DOCUMENT_ID, jsonNode1);
    PatchSet patchSet = patchSet(REMOVE, "", null);

    JsonNode result = underTest.processPatch(patchSet);

    assertThat(result, is(MissingNode.getInstance()));
    assertThat(store.get(DOCUMENT_ID), is(nullValue()));
  }

  @Test(expected = PatchApplicationException.class)
  public void removeChildFromNonExistingRoot() throws PatchApplicationException {
    PatchSet patchSet = patchSet(REMOVE, "/goo", null);

    underTest.processPatch(patchSet);
  }

  @Test(expected = PatchApplicationException.class)
  public void removeChildFromExistingRoot() throws PatchApplicationException {
    store.put(DOCUMENT_ID, jsonNode1);
    PatchSet patchSet = patchSet(REMOVE, "/goo", null);

    underTest.processPatch(patchSet);
  }

  private PatchSet patchSet(Operation operation, String path, Map<String, String> value) {
    return new PatchSet(DOCUMENT_ID, singletonList(new PatchOperation(operation, path, value)));
  }

}
