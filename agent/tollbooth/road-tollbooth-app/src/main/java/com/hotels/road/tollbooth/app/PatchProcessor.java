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
package com.hotels.road.tollbooth.app;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;

import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;

@Component
@Slf4j
@RequiredArgsConstructor
class PatchProcessor {
  private final Map<String, JsonNode> store;
  private final JsonPatchApplier patchApplier;

  JsonNode processPatch(PatchSet patchSet) throws PatchApplicationException {
    String documentId = patchSet.getDocumentId();
    List<PatchOperation> operations = patchSet.getOperations();

    synchronized (store) {
      JsonNode document = store.getOrDefault(documentId, NullNode.getInstance());
      JsonNode updatedDocument = patchApplier.apply(document, operations);

      if (updatedDocument.isMissingNode()) {
        log.info("Removing document {}", documentId);
        store.remove(documentId);
      } else {
        log.info("Updating document {}", documentId);
        log.info("New document : {}", updatedDocument);
        store.put(documentId, updatedDocument);
      }
      return updatedDocument;
    }
  }

}
