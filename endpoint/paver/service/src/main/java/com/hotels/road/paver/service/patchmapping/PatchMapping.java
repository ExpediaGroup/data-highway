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

import static com.hotels.road.tollbooth.client.api.Operation.ADD;
import static com.hotels.road.tollbooth.client.api.Operation.REPLACE;

import com.hotels.road.model.core.Road;
import com.hotels.road.tollbooth.client.api.PatchOperation;

public abstract class PatchMapping {
  public abstract String getPath();

  public abstract PatchOperation convertOperation(Road road, PatchOperation modelOperation);

  protected boolean isAddOrReplace(PatchOperation operation) {
    return operation.getOperation() == ADD || operation.getOperation() == REPLACE;
  }
}
