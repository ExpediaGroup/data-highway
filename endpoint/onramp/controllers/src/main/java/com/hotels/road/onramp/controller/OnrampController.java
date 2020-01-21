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
package com.hotels.road.onramp.controller;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNPROCESSABLE_ENTITY;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.RoadUnavailableException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampService;
import com.hotels.road.rest.model.StandardResponse;

@Api(tags = "onramp")
@RestController
@RequestMapping("/onramp/v1")
@RequiredArgsConstructor
@Slf4j
public class OnrampController {
  private static final String MESSAGE_ACCEPTED = "Message accepted.";

  private final OnrampService service;
  private final MeterRegistry registry;

  @ApiOperation(value = "Sends a given array of messages to a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Messages have been sent successfully.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Bad Request.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class),
      @ApiResponse(code = 422, message = "Road not enabled.", response = StandardResponse.class) })
  @PreAuthorize("@onrampAuthorisation.isAuthorised(authentication,#roadName)")
  @PostMapping(path = "/roads/{roadName}/messages")
  public Iterable<StandardResponse> produce(@PathVariable String roadName, @RequestBody ArrayNode json)
    throws UnknownRoadException, InterruptedException {
    Timer.Sample sample = Timer.start(registry);
    DistributionSummary.builder("onramp.request").tag("road", roadName).register(registry).record(json.size());
    Onramp onramp = service.getOnramp(roadName).orElseThrow(() -> new UnknownRoadException(roadName));
    if (!onramp.isAvailable()) {
      throw new RoadUnavailableException(String.format("Road '%s' is disabled, could not send events.", roadName));
    }
    Iterable<StandardResponse> responses = sendMessages(onramp, json);
    sample.stop(registry.timer("onramp.request.timer", "road", roadName));
    return responses;
  }

  private Iterable<StandardResponse> sendMessages(Onramp onramp, Iterable<JsonNode> json) throws InterruptedException {
    return stream(json.spliterator(), false).map(onramp::sendEvent).map(this::translateFuture).collect(toList());
  }

  private StandardResponse translateFuture(Future<Boolean> future) {
    try {
      future.get();
      return StandardResponse.successResponse(MESSAGE_ACCEPTED);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (!(cause instanceof InvalidEventException)) {
        log.warn("Problem sending event", e);
      }
      return StandardResponse.failureResponse(cause.getMessage());
    } catch (InterruptedException e) {
      throw new ServiceException(e);
    }
  }

  @ExceptionHandler
  @ResponseStatus(UNPROCESSABLE_ENTITY)
  public StandardResponse roadUnavailable(RoadUnavailableException e) {
    return StandardResponse.failureResponse(e.getMessage());
  }

  @ExceptionHandler
  @ResponseStatus(NOT_FOUND)
  public StandardResponse unknownRoadException(UnknownRoadException e) {
    log.warn(NOT_FOUND.getReasonPhrase(), e);
    registry.counter("onramp.road_not_found").increment();
    return StandardResponse.failureResponse(e.getMessage());
  }
}
