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
package com.hotels.road.paver.service.patchmapping;

import static com.google.common.base.Preconditions.checkArgument;

import static com.hotels.road.tollbooth.client.api.Operation.REMOVE;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.hotels.road.model.core.Road;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@Component
public class OfframpAuthPatchMapping extends PatchMapping {
  static final String PATH = "/authorisation/offramp/authorities";

  @Override
  public String getPath() {
    return PATH;
  }

  @Override
  public PatchOperation convertOperation(Road road, PatchOperation modelOperation) {
    checkArgument(modelOperation.getOperation() != REMOVE, "only add or replace are supported");
    checkArgument(modelOperation.getValue() instanceof Map, "value must be a map");
    Map<?, ?> map = (Map<?, ?>) modelOperation.getValue();
    if (!map.isEmpty()) {
      Object firstItem = map.values().iterator().next();
      checkArgument(firstItem instanceof List, "map value must be an array");
      List<?> list = (List<?>) firstItem;
      checkArgument(!list.isEmpty(), "array type must not be empty");
      checkArgument(list.get(0) instanceof String, "array type must be a string");
    }
    return PatchOperation.replace(PATH, modelOperation.getValue());
  }
}
