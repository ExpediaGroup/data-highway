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

import static com.hotels.road.paver.controller.PaverConstants.CONTEXT_PATH;

import java.util.List;
import java.util.SortedSet;

import javax.servlet.http.HttpServletRequest;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.paver.service.PaverService;
import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.rest.model.RoadModel;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.tollbooth.client.api.PatchOperation;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;

@Api(tags = "road")
@RestController
@RequestMapping(CONTEXT_PATH + "/roads")
@RequiredArgsConstructor
public class RoadController {
  private final PaverService service;

  @ApiOperation(value = "Lists all road names")
  // The array of api responses is required. Without it, documentation is not generated correctly in paths.md file.
  @ApiResponses({ @ApiResponse(code = 200, message = "List of all road names", response = StandardResponse.class) })
  @GetMapping
  public SortedSet<String> listRoads(HttpServletRequest req) {
    return service.getRoadNames();
  }

  @ApiOperation(value = "Creates a new road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Request to create a road received.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Invalid request or road name.", response = StandardResponse.class),
      @ApiResponse(code = 415, message = "Unsupported media type. Only 'application/json' content type is supported.", response = StandardResponse.class),
      @ApiResponse(code = 409, message = "Road already exists.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @PostMapping
  public StandardResponse createRoad(@RequestBody BasicRoadModel road) {
    service.createRoad(road);
    return StandardResponse.successResponse(String.format("Request to create road \"%s\" received", road.getName()));
  }

  @ApiOperation(value = "Returns the details of a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Details of a road returned successfully.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Invalid request or road name.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class) })
  @GetMapping("/{name}")
  public RoadModel getRoad(@ApiParam(name = "name", value = "road name", required = true) @PathVariable String name)
    throws UnknownRoadException {
    return service.getRoad(name);
  }

  @ApiOperation(value = "Applies JSON Patch modifications to a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Patch applied successfully", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Invalid patch", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @RequestMapping(value = "/{name}", method = {
      RequestMethod.PATCH,
      RequestMethod.PUT }, consumes = "application/json-patch+json")
  public StandardResponse patchRoad(
      @ApiParam(name = "name", value = "road name", required = true) @PathVariable String name,
      @RequestBody List<PatchOperation> patchSet)
    throws UnknownRoadException {
    service.applyPatch(name, patchSet);
    return StandardResponse.successResponse("Patch applied");
  }

  @ApiOperation(value = "Deletes a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Delete a road returned successfully.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Invalid request or road name.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @DeleteMapping("/{name}")
  public StandardResponse delete(@ApiParam(name = "name", value = "road name", required = true) @PathVariable String name)
    throws UnknownRoadException {
    service.deleteRoad(name);
    return StandardResponse
        .successResponse(String.format("Request to delete Road for \"%s\" received.", name));
  }
}
