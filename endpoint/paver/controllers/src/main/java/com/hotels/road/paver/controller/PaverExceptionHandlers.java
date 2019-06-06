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

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.NOT_FOUND;

import javax.servlet.http.HttpServletRequest;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.paver.service.exception.NoSuchSchemaException;
import com.hotels.road.rest.model.StandardResponse;

@ControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class PaverExceptionHandlers {

  @ExceptionHandler({ UnknownRoadException.class, NoSuchSchemaException.class })
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public ResponseEntity<StandardResponse> notFoundHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(NOT_FOUND).body(StandardResponse.failureResponse(e.getMessage()));
  }

  @ExceptionHandler(AlreadyExistsException.class)
  @ResponseStatus(HttpStatus.CONFLICT)
  public ResponseEntity<StandardResponse> alreadyExistsExceptionHandler(HttpServletRequest request, Exception e) {
    return ResponseEntity.status(CONFLICT).body(StandardResponse.failureResponse("Road already exists."));
  }
}
