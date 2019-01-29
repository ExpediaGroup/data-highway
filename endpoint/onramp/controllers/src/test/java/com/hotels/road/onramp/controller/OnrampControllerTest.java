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
package com.hotels.road.onramp.controller;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;

import java.util.Optional;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampService;
import com.hotels.road.rest.controller.common.GlobalExceptionHandler;
import com.hotels.road.security.CidrBlockAuthorisation;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
    OnrampController.class,
    GlobalExceptionHandler.class,
    OnrampControllerTest.TestConfiguration.class })
public class OnrampControllerTest {
  @Configuration
  public static class TestConfiguration {
    public static final MeterRegistry registry = new SimpleMeterRegistry();

    public @Bean MeterRegistry testMeterRegistry() {
      return registry;
    }
  }

  private static final String INVALID_EVENT_DETAIL = "Invalid Event";
  private static final String ERROR_SENT = "An error occured while posting an event";
  private static final String NON_EXISTENT_ROAD = "non_existent_road";
  private static final String PRESENT_ROAD = "present_road";
  private static final String CLOSED_ROAD = "closed_road";
  private static final String ENDPOINT_URI_FORMAT = "/onramp/v1/roads/%s/messages";
  private static final String NON_EXISTENT_ROAD_URI = String.format(ENDPOINT_URI_FORMAT, NON_EXISTENT_ROAD);
  private static final String PRESENT_ROAD_URI = String.format(ENDPOINT_URI_FORMAT, PRESENT_ROAD);
  private static final String CLOSED_ROAD_URI = String.format(ENDPOINT_URI_FORMAT, CLOSED_ROAD);

  @MockBean
  private CidrBlockAuthorisation authorisation;
  @MockBean
  private OnrampService onrampService;
  @Mock
  private Onramp onramp;
  @Mock
  private Onramp errorOnramp;
  @Mock
  private Onramp closedOnramp;
  @Mock
  private SchemaVersion schemaVersion;

  private MockMvc mockMvc;

  @Autowired
  private OnrampController onrampController;

  @Autowired
  private GlobalExceptionHandler globalExceptionHandler;

  @Before
  public void setup() {
    mockMvc = standaloneSetup(onrampController)
        .setControllerAdvice(globalExceptionHandler)
        .setMessageConverters(new MappingJackson2HttpMessageConverter())
        .build();
    when(onramp.sendEvent(any(JsonNode.class))).thenAnswer(a -> {
      if (((JsonNode) a.getArgument(0)).get("valid").asBoolean()) {
        return immediateFuture(true);
      } else {
        return immediateFailedFuture(new InvalidEventException(INVALID_EVENT_DETAIL));
      }
    });
    when(onramp.isAvailable()).thenReturn(true);
    when(onramp.getSchemaVersion()).thenReturn(schemaVersion);
    when(errorOnramp.sendEvent(any(JsonNode.class)))
        .thenReturn(immediateFailedFuture(new ServiceException(ERROR_SENT)));
    when(errorOnramp.isAvailable()).thenReturn(true);
    when(closedOnramp.isAvailable()).thenReturn(false);
  }

  @Test
  public void roadNotFound() throws Exception {
    given(onrampService.getOnramp(NON_EXISTENT_ROAD)).willReturn(Optional.empty());

    mockMvc
        .perform(post(NON_EXISTENT_ROAD_URI).content("[{}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message", is(String.format("Road \"%s\" does not exist.", NON_EXISTENT_ROAD))))
        .andExpect(jsonPath("$.success", is(false)));
  }

  @Test
  public void eventPostedSuccessfully() throws Exception {
    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(onramp));

    mockMvc
        .perform(post(PRESENT_ROAD_URI).content("[{\"valid\":true}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].message", is("Message accepted.")))
        .andExpect(jsonPath("$[0].success", is(true)));
  }

  @Test
  public void errorOnEventPost() throws Exception {
    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(errorOnramp));
    given(errorOnramp.getSchemaVersion()).willReturn(schemaVersion);

    mockMvc
        .perform(post(PRESENT_ROAD_URI).content("[{}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].message", is(ERROR_SENT)))
        .andExpect(jsonPath("$[0].success", is(false)));
  }

  @Test
  public void roadClosed() throws Exception {
    given(onrampService.getOnramp(CLOSED_ROAD)).willReturn(Optional.of(closedOnramp));

    mockMvc
        .perform(post(CLOSED_ROAD_URI).content("[{\"valid\":true}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isUnprocessableEntity())
        .andExpect(jsonPath("$.message", is("Road 'closed_road' is disabled, could not send events.")))
        .andExpect(jsonPath("$.success", is(false)));
  }

  @Test
  public void invalidEvent() throws Exception {
    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(onramp));

    mockMvc
        .perform(post(PRESENT_ROAD_URI).content("[{\"valid\":false}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].message", is("The event failed validation. Invalid Event")))
        .andExpect(jsonPath("$[0].success", is(false)));
  }

  @Test
  public void twoEventsSentSuccessfully() throws Exception {
    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(onramp));

    mockMvc
        .perform(
            post(PRESENT_ROAD_URI).content("[{\"valid\":true},{\"valid\":true}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].message", is("Message accepted.")))
        .andExpect(jsonPath("$[0].success", is(true)))
        .andExpect(jsonPath("$[1].message", is("Message accepted.")))
        .andExpect(jsonPath("$[1].success", is(true)));
  }

  @Test
  public void twoEventsFirstFailedSecondSucceeded() throws Exception {
    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(onramp));

    mockMvc
        .perform(
            post(PRESENT_ROAD_URI).content("[{\"valid\":false},{\"valid\":true}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].message", is("The event failed validation. Invalid Event")))
        .andExpect(jsonPath("$[0].success", is(false)))
        .andExpect(jsonPath("$[1].message", is("Message accepted.")))
        .andExpect(jsonPath("$[1].success", is(true)));
  }

  @Test
  public void notReadable() throws Exception {
    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(onramp));

    mockMvc
        .perform(post(PRESENT_ROAD_URI).content("notReadable").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", startsWith("Unable to parse message")))
        .andExpect(jsonPath("$.success", is(false)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void interruptedException() throws Exception {
    Onramp onramp = mock(Onramp.class);
    Future<Boolean> future = mock(Future.class);

    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(onramp));
    given(onramp.isAvailable()).willReturn(true);
    given(onramp.getSchemaVersion()).willReturn(schemaVersion);
    given(onramp.sendEvent(any(JsonNode.class))).willReturn(future);
    given(future.get()).willThrow(new InterruptedException("foo"));

    mockMvc
        .perform(
            post(PRESENT_ROAD_URI).content("[{\"valid\":false},{\"valid\":true}]").contentType(APPLICATION_JSON_UTF8))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", is("java.lang.InterruptedException: foo")))
        .andExpect(jsonPath("$.success", is(false)));
  }

  @Test
  public void notAnArray() throws Exception {
    given(onrampService.getOnramp(PRESENT_ROAD)).willReturn(Optional.of(onramp));

    mockMvc.perform(post(PRESENT_ROAD_URI).content("{\"valid\":true}").contentType(APPLICATION_JSON_UTF8)).andExpect(
        status().isBadRequest());
  }
}
