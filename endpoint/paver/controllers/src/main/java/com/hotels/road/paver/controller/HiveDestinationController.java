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
package com.hotels.road.paver.controller;

import static com.hotels.road.paver.controller.PaverConstants.CONTEXT_PATH;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.UnknownDestinationException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.paver.service.HiveDestinationService;
import com.hotels.road.paver.service.exception.InvalidLandingIntervalException;
import com.hotels.road.rest.model.HiveDestinationModel;
import com.hotels.road.rest.model.StandardResponse;

@Api(tags = "hive")
@RestController
@RequestMapping(CONTEXT_PATH + "/roads/{name}/destinations/hive")
@RequiredArgsConstructor
public class HiveDestinationController {
  private final HiveDestinationService hiveDestinationService;

  @ApiOperation(value = "Returns details of a Hive destination")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Details of a Hive destination.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road or Hive destination not found.", response = StandardResponse.class) })
  @GetMapping
  public HiveDestinationModel get(@PathVariable String name) throws UnknownRoadException, UnknownDestinationException {
    return hiveDestinationService.getHiveDestination(name);
  }

  @ApiOperation(value = "Creates a new Hive destination")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Creation of a new Hive destination requested.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class),
      @ApiResponse(code = 409, message = "Hive destination already exists.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @PostMapping
  public StandardResponse post(@PathVariable String name, @RequestBody HiveDestinationModel hiveDestinationModel)
    throws AlreadyExistsException, UnknownRoadException {
    hiveDestinationService.createHiveDestination(name, hiveDestinationModel);
    return StandardResponse
        .successResponse(String.format("Request to create Hive destination for \"%s\" received.", name));
  }

  @ApiOperation(value = "Updates an existing Hive destination")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Update of existing Hive destination requested.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road or Hive destination not found.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @PutMapping
  public StandardResponse put(@PathVariable String name, @RequestBody HiveDestinationModel hiveDestinationModel)
    throws UnknownRoadException, UnknownDestinationException {
    hiveDestinationService.updateHiveDestination(name, hiveDestinationModel);
    return StandardResponse
        .successResponse(String.format("Request to update Hive destination for \"%s\" received.", name));
  }

  @ApiOperation(value = "Deletes an existing Hive destination")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Deletion of existing Hive destination requested.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road or Hive destination not found.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @DeleteMapping
  public StandardResponse delete(@PathVariable String name)
      throws UnknownRoadException, UnknownDestinationException {
    hiveDestinationService.deleteHiveDestination(name);
    return StandardResponse
        .successResponse(String.format("Request to delete Hive destination for \"%s\" received.", name));
  }

  @ExceptionHandler(UnknownRoadException.class)
  public ResponseEntity<StandardResponse> unknownRoadExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(StandardResponse.failureResponse(e.getMessage()));
  }

  @ExceptionHandler(UnknownDestinationException.class)
  public ResponseEntity<StandardResponse> unknownDestinationExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(StandardResponse.failureResponse(e.getMessage()));
  }

  @ExceptionHandler(AlreadyExistsException.class)
  public ResponseEntity<StandardResponse> alreadyExistsExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(StandardResponse.failureResponse(e.getMessage()));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<StandardResponse> illegalArgumentExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(StandardResponse.failureResponse(e.getMessage()));
  }

  @ExceptionHandler(InvalidLandingIntervalException.class)
  public ResponseEntity<StandardResponse> invalidLandingIntervalExceptionHandler(
      HttpServletRequest request,
      Exception e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(StandardResponse.failureResponse(e.getMessage()));
  }
}
