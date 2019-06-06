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
package com.hotels.road.offramp.service;

import static com.hotels.road.rest.model.Sensitivity.PII;

import java.util.Set;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.SchemaProvider;
import com.hotels.road.rest.model.Sensitivity;

public interface MessageFunction extends Function<Payload<JsonNode>, JsonNode> {
  @Component
  @RequiredArgsConstructor
  public class Factory {
    private final SchemaProvider schemaProvider;
    private final PiiDataReplacer piiDataReplacer;

    public MessageFunction create(String roadName, Set<Sensitivity> grants) {
      if (grants.contains(PII)) {
        return Payload::getMessage;
      }
      return payload -> {
        Schema schema = schemaProvider.schema(roadName, payload.getSchemaVersion());
        return piiDataReplacer.replace(schema, payload.getMessage());
      };
    }
  }
}
