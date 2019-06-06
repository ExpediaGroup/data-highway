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

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;

import com.hotels.road.exception.InvalidKeyPathException;
import com.hotels.road.exception.InvalidSchemaException;
import com.hotels.road.exception.InvalidSchemaVersionException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.paver.service.PaverService;
import com.hotels.road.paver.service.exception.NoSuchSchemaException;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.schema.gdpr.InvalidPiiAnnotationException;

@Api(tags = "schema")
@RestController
@RequestMapping(CONTEXT_PATH + "/roads/{name}/schemas")
@RequiredArgsConstructor
public class SchemaController {
  private static final String LATEST_NOT_SUPPORTED_MESSAGE = "'latest' keyword is not supported when deleting schemas. Please specify concrete version instead.";
  static final String SCHEMA_DELETE_REQUEST = "Request to delete schema version %s received.";
  static final String DELETE_LATEST_SCHEMA_ERROR = "Only the latest schema can be deleted. Expected version: %s, got: %s";
  static final String LATEST = "latest";

  private final PaverService service;

  @ApiOperation(value = "Returns all schemas keyed by version number")
  @ApiResponses({
      @ApiResponse(code = 200, message = "List of schemas returned successfully.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class) })
  @GetMapping
  public Map<Integer, Schema> getSchemas(@PathVariable String name) throws UnknownRoadException {
    return service.getActiveSchemas(name);
  }

  @ApiOperation(value = "Adds a new schema to a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Request to add a new schema received.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class),
      @ApiResponse(code = 409, message = "Schema is not compatible with previous schemas.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @PostMapping
  public StandardResponse addNewSchema(
      @ApiParam(name = "name", value = "road name", required = true) @PathVariable String name,
      @RequestBody Schema schema)
    throws UnknownRoadException, InvalidKeyPathException {
    service.addSchema(name, schema);
    return StandardResponse.successResponse("Request to add a new schema received.");
  }

  @ApiOperation(value = "Adds a new schema with a specific version to a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Request to add a new schema received.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Invalid Schema Version. The requested version must be greater than the largest registered version.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class),
      @ApiResponse(code = 409, message = "Schema is not compatible with previous schemas.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @PostMapping("/{version}")
  public StandardResponse addNewSchema(
      @ApiParam(name = "name", value = "road name", required = true) @PathVariable String name,
      @PathVariable int version,
      @RequestBody Schema schema)
    throws UnknownRoadException, InvalidKeyPathException, InvalidSchemaVersionException {
    service.addSchema(name, schema, version);
    return StandardResponse.successResponse("Request to add a new schema received.");
  }

  @ApiOperation(value = "Returns a specific version of a schema")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Requested schema returned successfully.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road or schema not found.", response = StandardResponse.class) })
  @GetMapping("/{version}")
  public Schema getSchema(
      @ApiParam(name = "name", value = "road name", required = true) @PathVariable String name,
      @PathVariable String version)
    throws NoSuchSchemaException, UnknownRoadException {
    SchemaVersion schema;
    if (LATEST.equalsIgnoreCase(version)) {
      schema = service.getLatestActiveSchema(name);
    } else {
      schema = service.getActiveSchema(name, Integer.parseInt(version));
    }
    return schema.getSchema();
  }

  @ApiOperation(value = "Deletes the latest version of a schema")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Request to delete a schema received.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Invalid Schema Version. Only the latest schema can be deleted.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road or Schema not found.", response = StandardResponse.class) })
  @PreAuthorize("@paverAuthorisation.isAuthorised(authentication)")
  @DeleteMapping("/{version}")
  public ResponseEntity<StandardResponse> deleteLatestSchema(
      @ApiParam(name = "name", value = "road name", required = true) @PathVariable String name,
      @PathVariable String version)
    throws UnknownRoadException, NoSuchSchemaException {
    if (LATEST.equalsIgnoreCase(version)) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
          StandardResponse.failureResponse(LATEST_NOT_SUPPORTED_MESSAGE));
    }

    int versionInt = Integer.parseInt(version);

    SchemaVersion latestSchemaVersion = service.getLatestActiveSchema(name);
    if (versionInt == latestSchemaVersion.getVersion()) {
      service.deleteSchemaVersion(name, versionInt);
      return ResponseEntity.status(HttpStatus.OK).body(
          StandardResponse.successResponse(String.format(SCHEMA_DELETE_REQUEST, versionInt)));
    } else {
      String message = String.format(DELETE_LATEST_SCHEMA_ERROR, latestSchemaVersion.getVersion(), versionInt);
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(StandardResponse.failureResponse(message));
    }
  }

  @ExceptionHandler(AvroTypeException.class)
  public ResponseEntity<StandardResponse> avroTypeExceptionExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
        StandardResponse.failureResponse("Invalid schema. " + e.getMessage()));
  }

  @ExceptionHandler(SchemaParseException.class)
  public ResponseEntity<StandardResponse> schemaParseException(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
        StandardResponse.failureResponse("Invalid schema. " + e.getMessage()));
  }

  @ExceptionHandler(NumberFormatException.class)
  public ResponseEntity<StandardResponse> numberFormatExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(
        StandardResponse.failureResponse("Invalid version number. " + e.getMessage()));
  }

  @ExceptionHandler(InvalidSchemaException.class)
  public ResponseEntity<StandardResponse> invalidSchemaExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(
        StandardResponse.failureResponse("Invalid schema. " + e.getMessage()));
  }

  @ExceptionHandler(InvalidSchemaVersionException.class)
  public ResponseEntity<StandardResponse> invalidSchemaVersionException(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
        StandardResponse.failureResponse("Invalid schema version. " + e.getMessage()));
  }

  @ExceptionHandler(InvalidPiiAnnotationException.class)
  public ResponseEntity<StandardResponse> invalidPiiAnnotationException(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(StandardResponse.failureResponse(e.getMessage()));
  }
}
