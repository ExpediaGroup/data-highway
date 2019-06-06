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

import org.springframework.stereotype.Component;

import com.hotels.road.model.core.Road;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@Component
public class EnabledPatchMapping extends PatchMapping {
  @Override
  public String getPath() {
    return "/enabled";
  }

  @Override
  public PatchOperation convertOperation(Road road, PatchOperation modelOperation) {
    checkArgument(isAddOrReplace(modelOperation), "You can only change the value of enabled");
    checkArgument(modelOperation.getValue() instanceof Boolean, "enabled must be a boolean");
    if (isTryingToEnableRoad(modelOperation)) {
      checkArgument(road.getStatus().isTopicCreated(), "Road is not ready and cannot be enabled");
    }

    return PatchOperation.replace("/enabled", modelOperation.getValue());
  }

  private boolean isTryingToEnableRoad(PatchOperation modelOperation) {
    return (Boolean) (modelOperation.getValue());
  }
}
