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
package com.hotels.road.paver.controller;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import static com.hotels.road.paver.controller.PaverConstants.CONTEXT_PATH;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.UnknownDestinationException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.paver.service.HiveDestinationService;
import com.hotels.road.paver.service.exception.InvalidLandingIntervalException;
import com.hotels.road.rest.controller.common.GlobalExceptionHandler;
import com.hotels.road.rest.model.HiveDestinationModel;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
    HiveDestinationController.class,
    PaverExceptionHandlers.class,
    GlobalExceptionHandler.class })
public class HiveDestinationControllerTest {
  private static final String HIVE_DESTINATION_ENDPOINT = CONTEXT_PATH + "/roads/road1/destinations/hive";
  private static final String DESTINATION_JSON = "{}";

  @MockBean
  private HiveDestinationService service;

  @Autowired
  private HiveDestinationController underTest;
  @Autowired
  private PaverExceptionHandlers defaultExceptionHandler;
  @Autowired
  private GlobalExceptionHandler globalExceptionHandler;

  private MockMvc mockMvc;

  private static final String NAME = "road1";

  private HiveDestinationModel hiveDestinationModel;

  @Before
  public void before() {
    hiveDestinationModel = new HiveDestinationModel();

    mockMvc = standaloneSetup(underTest).setControllerAdvice(defaultExceptionHandler, globalExceptionHandler).build();
  }

  @Test
  public void get_Ok() throws Exception {
    hiveDestinationModel.setLandingInterval("PT1H");
    when(service.getHiveDestination(NAME)).thenReturn(hiveDestinationModel);

    mockMvc
        .perform(get(HIVE_DESTINATION_ENDPOINT))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.enabled", is(false)))
        .andExpect(jsonPath("$.landingInterval", is("PT1H")));
  }

  @Test
  public void get_UnknownRoad() throws Exception {
    doThrow(new UnknownRoadException(NAME)).when(service).getHiveDestination(NAME);

    mockMvc
        .perform(get(HIVE_DESTINATION_ENDPOINT))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("Road \"road1\" does not exist.")));
  }

  @Test
  public void get_UnknownDestination() throws Exception {
    doThrow(new UnknownDestinationException("Hive", NAME)).when(service).getHiveDestination(NAME);

    mockMvc
        .perform(get(HIVE_DESTINATION_ENDPOINT))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("Hive destination for Road \"road1\" does not exist.")));
  }

  @Test
  public void post_Ok() throws Exception {
    mockMvc
        .perform(post(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(true)))
        .andExpect(jsonPath("$.message", is("Request to create Hive destination for \"road1\" received.")));

    verify(service).createHiveDestination(NAME, hiveDestinationModel);
  }

  @Test
  public void post_Ok_enabled_customInterval() throws Exception {
    String destinationJson = "{\"enabled\" : true, \"landingInterval\" : \"PT30M\"}";
    mockMvc
        .perform(post(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(destinationJson))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(true)))
        .andExpect(jsonPath("$.message", is("Request to create Hive destination for \"road1\" received.")));

    hiveDestinationModel.setEnabled(true);
    hiveDestinationModel.setLandingInterval("PT30M");
    verify(service).createHiveDestination(NAME, hiveDestinationModel);
  }

  @Test
  public void post_UnknownRoad() throws Exception {
    doThrow(new UnknownRoadException(NAME)).when(service).createHiveDestination(NAME, hiveDestinationModel);

    mockMvc
        .perform(post(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("Road \"road1\" does not exist.")));
  }

  @Test
  public void post_DesitationAlreadyExists() throws Exception {
    doThrow(new AlreadyExistsException("foo")).when(service).createHiveDestination(NAME, hiveDestinationModel);

    mockMvc
        .perform(post(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isConflict())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("foo")));
  }

  @Test
  public void post_InvalidLandingIntervalException() throws Exception {
    doThrow(InvalidLandingIntervalException.class).when(service).createHiveDestination(NAME, hiveDestinationModel);

    mockMvc
        .perform(post(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is(nullValue())));
  }

  @Test
  public void put_Ok() throws Exception {
    mockMvc
        .perform(put(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(true)))
        .andExpect(jsonPath("$.message", is("Request to update Hive destination for \"road1\" received.")));

    verify(service).updateHiveDestination(NAME, hiveDestinationModel);
  }

  @Test
  public void put_Ok_enabled_customInterval() throws Exception {
    String destinationJson = "{\"enabled\" : true, \"landingInterval\" : \"PT30M\"}";
    mockMvc
        .perform(put(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(destinationJson))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(true)))
        .andExpect(jsonPath("$.message", is("Request to update Hive destination for \"road1\" received.")));

    hiveDestinationModel.setEnabled(true);
    hiveDestinationModel.setLandingInterval("PT30M");
    verify(service).updateHiveDestination(NAME, hiveDestinationModel);
  }

  @Test
  public void put_InvalidLandingIntervalException() throws Exception {
    doThrow(InvalidLandingIntervalException.class).when(service).updateHiveDestination(NAME, hiveDestinationModel);

    mockMvc
        .perform(put(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is(nullValue())));
  }

  @Test
  public void put_UnknownRoad() throws Exception {
    doThrow(new UnknownRoadException(NAME)).when(service).updateHiveDestination(NAME, hiveDestinationModel);

    mockMvc
        .perform(put(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("Road \"road1\" does not exist.")));
  }

  @Test
  public void post_UnknownDestination() throws Exception {
    doThrow(new UnknownDestinationException("Hive", NAME)).when(service).updateHiveDestination(NAME,
        hiveDestinationModel);

    mockMvc
        .perform(put(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("Hive destination for Road \"road1\" does not exist.")));
  }

  @Test
  public void delete_UnknownRoad() throws Exception {
    doThrow(new UnknownRoadException(NAME)).when(service).deleteHiveDestination(NAME);

    mockMvc
        .perform(delete(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("Road \"road1\" does not exist.")));
  }

  @Test
  public void delete_UnknownDestination() throws Exception {
    doThrow(new UnknownDestinationException("Hive", NAME)).when(service).deleteHiveDestination(NAME);

    mockMvc
        .perform(delete(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(false)))
        .andExpect(jsonPath("$.message", is("Hive destination for Road \"road1\" does not exist.")));
  }

  @Test
  public void delete_Ok() throws Exception {
    mockMvc
        .perform(delete(HIVE_DESTINATION_ENDPOINT).contentType(APPLICATION_JSON_UTF8).content(DESTINATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.timestamp", isA(Long.TYPE)))
        .andExpect(jsonPath("$.success", is(true)))
        .andExpect(jsonPath("$.message", is("Request to delete Hive destination for \"road1\" received.")));

    verify(service).deleteHiveDestination(NAME);
  }
}
