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
package com.hotels.road.agents.trafficcop.spi;

import java.util.List;

import com.hotels.road.tollbooth.client.api.PatchOperation;

public interface Agent<M> {
  List<PatchOperation> newModel(String key, M newModel);

  List<PatchOperation> updatedModel(String key, M oldModel, M newModel);

  void deletedModel(String key, M oldModel);

  List<PatchOperation> inspectModel(String key, M model);
}
