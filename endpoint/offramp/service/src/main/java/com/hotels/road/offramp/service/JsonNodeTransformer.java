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

import java.util.List;
import java.util.function.Function;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;

@RequiredArgsConstructor
public class JsonNodeTransformer {
  private static final String UNION_PATTERN = "/[0-9]+/";
  private static final String COLLECTION_SYMBOL = "*";
  private static final Splitter collectionSplitter = Splitter.onPattern("/\\*").limit(2);
  static final byte[] EMPTY = new byte[] {};
  private final Function<JsonNode, JsonNode> function;

  public JsonNode transform(JsonNode jsonNode, List<String> pointers) {
    pointers.forEach(p -> transform(jsonNode, p.replaceAll(UNION_PATTERN, "/")));
    return jsonNode;
  }

  private JsonNode transform(JsonNode jsonNode, String pointer) {
    if (pointer.contains(COLLECTION_SYMBOL)) {
      List<String> paths = collectionSplitter.splitToList(pointer);
      jsonNode.at(paths.get(0)).iterator().forEachRemaining(v -> transform(v, paths.get(1)));
    } else {
      transformLeafPath(jsonNode, pointer);
    }
    return jsonNode;
  }

  private void transformLeafPath(JsonNode jsonNode, String pointer) {
    int index = pointer.lastIndexOf("/");
    String fieldName = pointer.substring(index + 1, pointer.length());
    JsonNode fieldNode = jsonNode.at(pointer);
    String parentNodePath = pointer.substring(0, index);
    JsonNode parentNode = jsonNode.at(parentNodePath);
    if (!parentNode.isMissingNode() && !fieldNode.isMissingNode()) {
      ObjectNode objectParentNode = (ObjectNode) parentNode;
      objectParentNode.set(fieldName, function.apply(fieldNode));
    }
  }
}
