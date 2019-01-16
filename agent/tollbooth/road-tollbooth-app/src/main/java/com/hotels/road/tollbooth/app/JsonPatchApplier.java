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
package com.hotels.road.tollbooth.app;

import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;

import com.hotels.road.tollbooth.client.api.PatchOperation;

@Component
@RequiredArgsConstructor
public class JsonPatchApplier {
  private final ObjectMapper mapper;

  public JsonNode apply(JsonNode document, List<PatchOperation> patchOperations) throws PatchApplicationException {
    try {
      JsonNode patch = mapper.convertValue(patchOperations, JsonNode.class);
      JsonPatch jsonPatch = JsonPatch.fromJson(patch);

      return jsonPatch.apply(document);
    } catch (IOException | JsonPatchException e) {
      String message = String.format("Unable to apply patch to document. document: %s, patch: %s", document,
          patchOperations);
      throw new PatchApplicationException(message, e);
    }
  }
}
