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

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import com.hotels.road.tollbooth.client.api.PatchSet;

@RestController
public class RootController {
  private final Map<String, JsonNode> store;
  private final PatchProcessor patchProcessor;

  @Autowired
  public RootController(@Value("#{store}") Map<String, JsonNode> store, PatchProcessor patchProcessor) {
    this.store = store;
    this.patchProcessor = patchProcessor;
  }

  @GetMapping(path = "/", produces = APPLICATION_JSON_VALUE)
  public Map<String, JsonNode> get() throws JsonProcessingException {
    return store;
  }

  @GetMapping(path = "/{id}", produces = APPLICATION_JSON_VALUE)
  public JsonNode get(@PathVariable String id) throws DocumentNotFoundException {
    return Optional.ofNullable(store.get(id)).orElseThrow(documentNotFound(id));
  }

  @PostMapping(path = "/", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
  public JsonNode post(@RequestBody PatchSet patchSet) throws PatchApplicationException {
    return patchProcessor.processPatch(patchSet);
  }

  @DeleteMapping(path = "/{id}", produces = APPLICATION_JSON_VALUE)
  public JsonNode delete(@PathVariable String id) throws DocumentNotFoundException {
    return Optional
        .ofNullable(store.remove(id))
        .map(x -> messageResponse(String.format("Document '%s' was deleted.", id)))
        .orElseThrow(documentNotFound(id));
  }

  private Supplier<DocumentNotFoundException> documentNotFound(String id) {
    return () -> new DocumentNotFoundException(id);
  }

  @ExceptionHandler
  public ResponseEntity<JsonNode> handle(DocumentNotFoundException e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(messageResponse(e.getMessage()));
  }

  @ExceptionHandler
  public ResponseEntity<JsonNode> handle(PatchApplicationException e) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(messageResponse(e.getMessage()));
  }

  private JsonNode messageResponse(String message) {
    return JsonNodeFactory.instance.objectNode().put("message", message);
  }

  public static class DocumentNotFoundException extends Exception {
    private static final long serialVersionUID = 1L;

    DocumentNotFoundException(String id) {
      super(String.format("Document '%s' was not found.", id));
    }
  }

}
