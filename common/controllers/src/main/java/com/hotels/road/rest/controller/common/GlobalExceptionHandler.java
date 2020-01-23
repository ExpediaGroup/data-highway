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
package com.hotels.road.rest.controller.common;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.UNSUPPORTED_MEDIA_TYPE;
import static org.springframework.http.ResponseEntity.status;

import static com.hotels.road.rest.model.StandardResponse.failureResponse;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import com.hotels.road.exception.ServiceException;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.rest.model.validator.ModelValidationException;

import io.micrometer.core.instrument.MeterRegistry;

@ControllerAdvice
@Slf4j
@RequiredArgsConstructor
public class GlobalExceptionHandler {
  private final MeterRegistry registry;

  @ExceptionHandler
  public ResponseEntity<StandardResponse> accessDeniedException(AccessDeniedException e) {
    return status(FORBIDDEN).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> invalidRoadNameException(ModelValidationException e) {
    return status(BAD_REQUEST).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> illegalArgumentException(IllegalArgumentException e) {
    return status(BAD_REQUEST).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> jsonMappingException(JsonMappingException e) {
    return status(BAD_REQUEST).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> jsonParseException(JsonParseException e) {
    return status(BAD_REQUEST).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> unreadableMessage(HttpServletRequest request, HttpMessageNotReadableException e) {
    registry.counter("http.not.readable.exception", "request_uri", request.getRequestURI()).increment();
    log.error("Could not process request", e);
    return status(BAD_REQUEST).body(failureResponse("Unable to parse message: " + e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> mediaTypeNotSupported(HttpMediaTypeNotSupportedException e) {
    return status(UNSUPPORTED_MEDIA_TYPE).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> requestMethodNotSupported(HttpRequestMethodNotSupportedException e) {
    log.warn(BAD_REQUEST.getReasonPhrase(), e);
    return status(BAD_REQUEST).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> exception(Exception e) {
    log.warn(INTERNAL_SERVER_ERROR.getReasonPhrase(), e);
    return status(INTERNAL_SERVER_ERROR).body(failureResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<StandardResponse> serviceException(ServiceException e) {
    log.warn(INTERNAL_SERVER_ERROR.getReasonPhrase(), e);
    return status(INTERNAL_SERVER_ERROR).body(failureResponse(e.getMessage()));
  }
}
