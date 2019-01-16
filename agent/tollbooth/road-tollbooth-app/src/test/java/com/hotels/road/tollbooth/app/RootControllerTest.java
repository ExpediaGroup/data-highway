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
import static org.junit.Assert.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import static com.hotels.road.tollbooth.client.api.Operation.ADD;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.tollbooth.client.api.Operation;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;

public class RootControllerTest {

  private final Map<String, JsonNode> store = new HashMap<>();
  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonPatchApplier patchApplier = new JsonPatchApplier(mapper);
  private final PatchProcessor patchProcessor = new PatchProcessor(store, patchApplier);

  private final RootController rootController = new RootController(store, patchProcessor);

  private final Map<String, String> map1 = singletonMap("foo", "bar");
  private final Map<String, String> map2 = singletonMap("foo", "baz");
  private final JsonNode jsonNode1 = mapper.createObjectNode().put("foo", "bar");
  private final JsonNode jsonNode2 = mapper.createObjectNode().put("foo", "baz");

  private MockMvc mockMvc;

  @Before
  public void before() {
    mockMvc = standaloneSetup(rootController).build();
  }

  @Test
  public void getAll() throws Exception {
    store.put("doc1", jsonNode1);
    store.put("doc2", jsonNode2);

    mockMvc
        .perform(get("/"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.doc1", is(map1)))
        .andExpect(jsonPath("$.doc2", is(map2)))
        .andReturn();
  }

  @Test
  public void getId() throws Exception {
    store.put("doc1", jsonNode1);

    mockMvc
        .perform(get("/doc1"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$", is(map1)));
  }

  @Test
  public void getIdNotExists() throws Exception {
    mockMvc
        .perform(get("/doc1"))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Document 'doc1' was not found.")));
  }

  @Test
  public void postNew() throws Exception {
    PatchSet patchSet = patchSet("doc1", ADD, "", map1);
    mockMvc
        .perform(post("/").contentType(APPLICATION_JSON_UTF8).content(mapper.writeValueAsString(patchSet)))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$", is(map1)));

    assertThat(store.get("doc1"), is(jsonNode1));
  }

  @Test
  public void deleteExisting() throws Exception {
    store.put("doc1", jsonNode1);

    mockMvc
        .perform(delete("/doc1"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Document 'doc1' was deleted.")));

    assertThat(store.containsKey("doc1"), is(false));
  }

  @Test
  public void deleteNotExists() throws Exception {
    mockMvc
        .perform(delete("/doc1"))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Document 'doc1' was not found.")));
  }

  private PatchSet patchSet(String documentId, Operation operation, String path, Map<String, String> value) {
    return new PatchSet(documentId, singletonList(new PatchOperation(operation, path, value)));
  }

}
