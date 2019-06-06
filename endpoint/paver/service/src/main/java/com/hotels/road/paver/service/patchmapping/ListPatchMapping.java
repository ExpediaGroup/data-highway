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

import lombok.RequiredArgsConstructor;

import com.hotels.road.model.core.Road;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@RequiredArgsConstructor
public class ListPatchMapping extends PatchMapping {
  private final String path;
  private final Class<?> type;

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public PatchOperation convertOperation(Road road, PatchOperation modelOperation) {
    checkArgument(modelOperation.getOperation() != REMOVE, "only add or replace are supported");
    checkArgument(modelOperation.getValue() instanceof List, "value must be an array");
    List<?> list = (List<?>) modelOperation.getValue();
    if (!list.isEmpty()) {
      Object first = list.get(0);
      checkArgument(type.equals(first.getClass()), "array type must be a " + type.getSimpleName());
    }
    return PatchOperation.replace(path, modelOperation.getValue());
  }
}
