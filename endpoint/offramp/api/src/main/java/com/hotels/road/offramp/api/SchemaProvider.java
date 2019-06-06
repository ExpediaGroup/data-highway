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
package com.hotels.road.offramp.api;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.avro.Schema;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;

@RequiredArgsConstructor
public class SchemaProvider {
  private final Map<String, Road> store;

  public Schema schema(String roadName, int version) {
    return Mono
        .justOrEmpty(store.get(roadName))
        .switchIfEmpty(throwRoadDoesNotExist(roadName))
        .map(Road::getSchemas)
        .flatMap(schemas -> Mono.justOrEmpty(schemas.get(version)))
        .switchIfEmpty(throwSchemaDoesNotExist(roadName, version))
        .map(SchemaVersion::getSchema)
        .block();
  }

  private <T> Mono<T> throwRoadDoesNotExist(String roadName) {
    return defer(() -> new IllegalArgumentException("Road does not exist: " + roadName));
  }

  private <T> Mono<T> throwSchemaDoesNotExist(String roadName, int version) {
    return defer(() -> new IllegalArgumentException("Schema " + version + " does not exist for road: " + roadName));
  }

  private <T> Mono<T> defer(Supplier<RuntimeException> s) {
    return Mono.fromSupplier(() -> {
      throw s.get();
    });
  }
}
