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
package com.hotels.road.loadingbay;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import com.hotels.road.tollbooth.client.api.PatchOperation;

public class HiveStatusPatchBuilder {

  private final Builder<PatchOperation> operations = ImmutableList.builder();

  public HiveStatusPatchBuilder set(AgentProperty property, Object value) {
    operations.add(PatchOperation.replace(property.path(), value));
    return this;
  }

  public List<PatchOperation> build() {
    return operations.build();
  }

  public static HiveStatusPatchBuilder patchBuilder() {
    return new HiveStatusPatchBuilder();
  }

}
