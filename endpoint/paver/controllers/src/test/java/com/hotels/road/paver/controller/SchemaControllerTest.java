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

import static java.util.Collections.emptyMap;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import static com.google.common.collect.ImmutableMap.of;

import static com.hotels.road.paver.controller.PaverConstants.CONTEXT_PATH;

import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.exception.InvalidSchemaVersionException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.paver.service.PaverService;
import com.hotels.road.paver.service.exception.NoSuchSchemaException;
import com.hotels.road.rest.controller.common.GlobalExceptionHandler;
import com.hotels.road.rest.model.RoadModel;
import com.hotels.road.rest.model.RoadType;
import com.hotels.road.schema.gdpr.InvalidPiiAnnotationException;
import com.hotels.road.schema.serde.SchemaSerializationModule;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {
    SchemaController.class,
    PaverExceptionHandlers.class,
    GlobalExceptionHandler.class,
    SchemaSerializationModule.class,
    SimpleMeterRegistry.class})
public class SchemaControllerTest {
  private static final String ROAD_NAME = "road1";

  @MockBean
  private PaverService paverService;
  @Autowired
  private SchemaController schemaController;

  @Autowired
  private PaverExceptionHandlers paverExceptionHandler;
  @Autowired
  private GlobalExceptionHandler globalExceptionHandler;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private MockMvc mockMvc;

  private final Schema schema = SchemaBuilder
      .record("r")
      .fields()
      .name("f")
      .type()
      .booleanType()
      .noDefault()
      .endRecord();

  private final SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);
  private final SchemaVersion schemaVersion2 = new SchemaVersion(schema, 2, false);

  private String schemaJson;
  private RoadModel road;

  @Before
  public void setup() throws Exception {
    road = new RoadModel("road1", RoadType.NORMAL, "my road description", "my team", "a@b.c", true, null, null,
        of("foo", "bar"), true, Road.DEFAULT_COMPATIBILITY_MODE, emptyMap());

    objectMapper.registerModule(new SchemaSerializationModule());

    schemaJson = objectMapper.writeValueAsString(schema);
    mockMvc = standaloneSetup(schemaController)
        .setControllerAdvice(paverExceptionHandler, globalExceptionHandler)
        .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
        .build();

    when(paverService.getActiveSchema(any(String.class), anyInt())).thenReturn(schemaVersion);
    when(paverService.getRoad(ROAD_NAME)).thenReturn(road);
  }

  @Test
  public void getSchemas() throws Exception {
    Map<Integer, Schema> schemas = Collections.singletonMap(1, schema);
    when(paverService.getActiveSchemas(ROAD_NAME)).thenReturn(schemas);

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads/road1/schemas"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(content().string(objectMapper.writeValueAsString(schemas)));
  }

  @Test
  public void addNewSchema() throws Exception {
    SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);
    when(paverService.addSchema(ROAD_NAME, schema)).thenReturn(schemaVersion);
    when(paverService.getRoad(ROAD_NAME)).thenReturn(road);

    mockMvc
        .perform(post(CONTEXT_PATH + "/roads/road1/schemas").contentType(APPLICATION_JSON_UTF8).content(schemaJson))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.success", is(true)))
        .andExpect(jsonPath("$.message", is("Request to add a new schema received.")));

    verify(paverService).addSchema(ROAD_NAME, schema);
  }

  @Test
  public void addNewSchemaWithVersion() throws Exception {
    SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);
    when(paverService.addSchema(ROAD_NAME, schema, 1)).thenReturn(schemaVersion);
    when(paverService.getRoad(ROAD_NAME)).thenReturn(road);

    mockMvc
        .perform(post(CONTEXT_PATH + "/roads/road1/schemas/1").contentType(APPLICATION_JSON_UTF8).content(schemaJson))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.success", is(true)))
        .andExpect(jsonPath("$.message", is("Request to add a new schema received.")));

    verify(paverService).addSchema(ROAD_NAME, schema, 1);
  }

  @Test
  public void addNewSchemaWithVersion_InvalidSchemaVersionException() throws Exception {
    doThrow(new InvalidSchemaVersionException("foo")).when(paverService).addSchema(ROAD_NAME, schema, 1);

    mockMvc
        .perform(post(CONTEXT_PATH + "/roads/road1/schemas/1").contentType(APPLICATION_JSON_UTF8).content(schemaJson))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Invalid schema version. foo")));
  }

  @Test
  public void getLatest() throws Exception {
    when(paverService.getLatestActiveSchema(ROAD_NAME)).thenReturn(schemaVersion);

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads/road1/schemas/latest"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(content().string(schemaJson));
  }

  @Test
  public void getVersion() throws Exception {
    when(paverService.getActiveSchema(ROAD_NAME, 1)).thenReturn(schemaVersion);

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads/road1/schemas/1"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(content().string(schemaJson));
  }

  @Test
  public void notAValidVersion() throws Exception {
    mockMvc
        .perform(get(CONTEXT_PATH + "/roads/road1/schemas/x"))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Invalid version number. For input string: \"x\"")));
  }

  @Test
  public void noSchemaFound() throws Exception {
    when(paverService.getLatestActiveSchema(ROAD_NAME)).thenThrow(new NoSuchSchemaException(ROAD_NAME, 1));

    mockMvc
        .perform(get(CONTEXT_PATH + "/roads/road1/schemas/latest"))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Road \"road1\" has no schema version 1.")));
  }

  @Test
  public void invalidDefaultInSchema() throws Exception {
    mockMvc
        .perform(post(CONTEXT_PATH + "/roads/road1/schemas").contentType(APPLICATION_JSON_UTF8).content(
            "{\"type\":\"record\",\"name\":\"mine\",\"fields\":[{\"name\":\"str\",\"type\":\"int\",\"default\":\"0\"}]}"))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Invalid schema. Invalid default for field str: \"0\" not a \"int\"")));
  }

  @Test
  public void deleteSchemaVersionThatIsTheLatest() throws Exception {
    when(paverService.getLatestActiveSchema(ROAD_NAME)).thenReturn(schemaVersion2);
    mockMvc
        .perform(delete(CONTEXT_PATH + "/roads/road1/schemas/2"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is(String.format(SchemaController.SCHEMA_DELETE_REQUEST, 2))));
  }

  @Test
  public void deleteSchemaVersionThatIsNotTheLatest() throws Exception {
    when(paverService.getLatestActiveSchema(ROAD_NAME)).thenReturn(schemaVersion2);
    mockMvc
        .perform(delete(CONTEXT_PATH + "/roads/road1/schemas/1"))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is(String.format(SchemaController.DELETE_LATEST_SCHEMA_ERROR, 2, 1))));
  }

  @Test
  public void deleteLatestSchemaVersion() throws Exception {
    when(paverService.getLatestActiveSchema(ROAD_NAME)).thenReturn(schemaVersion);
    mockMvc.perform(delete(CONTEXT_PATH + "/roads/road1/schemas/latest")).andExpect(status().isBadRequest()).andExpect(
        content().contentType(APPLICATION_JSON_UTF8));
  }

  @Test
  public void addNewSchema_InvalidPiiAnnotationException() throws Exception {
    doThrow(new InvalidPiiAnnotationException("/foo")).when(paverService).addSchema(ROAD_NAME, schema, 1);

    mockMvc
        .perform(post(CONTEXT_PATH + "/roads/road1/schemas/1").contentType(APPLICATION_JSON_UTF8).content(schemaJson))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(APPLICATION_JSON_UTF8))
        .andExpect(jsonPath("$.message", is("Found illegal sensitivity annotation at /foo")));
  }
}
