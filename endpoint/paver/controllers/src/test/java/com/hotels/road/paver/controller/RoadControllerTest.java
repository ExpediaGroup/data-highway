/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.road.paver.controller;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import static com.google.common.collect.ImmutableMap.of;

import static com.hotels.road.paver.controller.PaverConstants.CONTEXT_PATH;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.Road;
import com.hotels.road.paver.service.PaverService;
import com.hotels.road.rest.controller.common.GlobalExceptionHandler;
import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.rest.model.RoadModel;
import com.hotels.road.rest.model.RoadType;
import com.hotels.road.tollbooth.client.api.PatchOperation;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { RoadController.class, PaverExceptionHandlers.class, GlobalExceptionHandler.class, SimpleMeterRegistry.class })
public class RoadControllerTest {
  @MockBean
  private PaverService paverService;

  @Autowired
  private RoadController roadController;
  @Autowired
  private PaverExceptionHandlers paverExceptionHandler;
  @Autowired
  private GlobalExceptionHandler globalExceptionHandler;

  private MockMvc mockMvc;

  private static final String NAME = "road1";

  private RoadModel road;
  private final ObjectMapper mapper = new ObjectMapper();

  @Before
  public void before() {
    road = new RoadModel(NAME, RoadType.NORMAL, "my road description", "my team", "a@b.c", true, null, null,
        of("foo", "bar"), true, Road.DEFAULT_COMPATIBILITY_MODE, null);

    mockMvc = standaloneSetup(roadController)
        .setControllerAdvice(paverExceptionHandler, globalExceptionHandler)
        .build();
  }

  @Test
  public void getRoad() throws Exception {
    when(paverService.getRoad(NAME)).thenReturn(road);

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads/road1"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.name", is(NAME)));
  }

  @Test
  public void getRoad_RoadDoesNotExist() throws Exception {
    doThrow(new UnknownRoadException("road1")).when(paverService).getRoad(NAME);

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads/road1"))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Road \"road1\" does not exist.")));
  }

  @Test
  public void updateRoad() throws Exception {
    when(paverService.getRoad(NAME)).thenReturn(road);

    String patchJson = mapper.writeValueAsString(singletonList(PatchOperation.replace("/enabled", Boolean.TRUE)));
    mockMvc
        .perform(patch(CONTEXT_PATH + "/roads/road1").contentType("application/json-patch+json").content(patchJson))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(content().json("{\"success\":true}"));

    verify(paverService).applyPatch(NAME, singletonList(PatchOperation.replace("/enabled", Boolean.TRUE)));
  }

  @Test
  public void updateRoad_RoadDoesNotExist() throws Exception {
    doThrow(new UnknownRoadException("road1")).when(paverService).applyPatch(anyString(), anyList());

    String patchJson = mapper.writeValueAsString(singletonList(PatchOperation.replace("/enabled", Boolean.TRUE)));
    mockMvc
        .perform(patch(CONTEXT_PATH + "/roads/road1").contentType("application/json-patch+json").content(patchJson))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(content().json("{\"success\":false, \"message\":\"Road \\\"road1\\\" does not exist.\"}"));
  }

  @Test
  public void returnsListFromStore() throws Exception {
    when(paverService.getRoadNames()).thenReturn(Sets.newTreeSet(Arrays.asList("road1", "road2")));

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.length()", is(2)))
        .andExpect(jsonPath("$[0]", is("road1")))
        .andExpect(jsonPath("$[1]", is("road2")));
  }

  @Test
  public void clientThrowsException() throws Exception {
    doThrow(new ServiceException("boo")).when(paverService).getRoadNames();

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads"))
        .andExpect(status().isInternalServerError())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("boo")));
  }

  @Test
  public void createRoad() throws Exception {
    String roadJson = mapper.writeValueAsString(road);
    mockMvc
        .perform(post(CONTEXT_PATH + "/roads").contentType(APPLICATION_JSON_UTF8).content(roadJson))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(content().json("{\"success\":true}"));

    verify(paverService).createRoad(
        new BasicRoadModel(road.getName(), road.getDescription(), road.getTeamName(), road.getContactEmail(),
            road.isEnabled(), road.getPartitionPath(), road.getAuthorisation(), road.getMetadata()));
  }

  @Test
  public void createRoad_RoadAlreadyExists() throws Exception {
    doThrow(AlreadyExistsException.class).when(paverService).createRoad(any(BasicRoadModel.class));

    String roadJson = mapper.writeValueAsString(road);
    mockMvc
        .perform(post(CONTEXT_PATH + "/roads").contentType(APPLICATION_JSON_UTF8).content(roadJson))
        .andExpect(status().isConflict())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Road already exists.")));
  }
}
