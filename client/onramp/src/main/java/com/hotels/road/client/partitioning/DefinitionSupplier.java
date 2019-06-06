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
package com.hotels.road.client.partitioning;

import static lombok.AccessLevel.PACKAGE;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.client.http.HttpHandler;
import com.hotels.road.client.OnrampOptions;

/** Fetches the road's model from the paver end point. */
@RequiredArgsConstructor(access = PACKAGE)
class DefinitionSupplier implements Supplier<JsonNode>, AutoCloseable {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final @NonNull HttpHandler handler;
  private final @NonNull String roadName;

  DefinitionSupplier(OnrampOptions options) {
    this(HttpHandler.paver(options.withThreads(1)), options.getRoadName());
  }

  @Override
  public JsonNode get() {
    try {
      HttpResponse response = handler.get("roads/" + roadName);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new RuntimeException("Error from Data Highway service: " + response.getStatusLine().toString());
      }
      return objectMapper.readTree(response.getEntity().getContent());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    handler.close();
  }
}
