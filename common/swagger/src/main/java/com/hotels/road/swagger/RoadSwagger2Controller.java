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
package com.hotels.road.swagger;

import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import springfox.documentation.annotations.ApiIgnore;
import springfox.documentation.spring.web.DocumentationCache;
import springfox.documentation.spring.web.json.Json;
import springfox.documentation.spring.web.json.JsonSerializer;
import springfox.documentation.swagger2.mappers.ServiceModelToSwagger2Mapper;

@RestController
@ApiIgnore
@RequiredArgsConstructor
public class RoadSwagger2Controller {
  private final DocumentationCache documentationCache;
  private final ServiceModelToSwagger2Mapper mapper;
  private final JsonSerializer jsonSerializer;

  @GetMapping("/{group}/{version}/swagger")
  public ResponseEntity<Json> getDocumentation(@PathVariable String group, @PathVariable String version) {
    return Optional
        .of(group)
        .map(documentationCache::documentationByGroup)
        .map(mapper::mapDocumentation)
        .map(jsonSerializer::toJson)
        .map(json -> new ResponseEntity<Json>(json, HttpStatus.OK))
        .orElse(new ResponseEntity<Json>(HttpStatus.BAD_REQUEST));
  }
}
