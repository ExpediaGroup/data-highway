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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.hotels.road.tollbooth.client.api.Operation;
import com.hotels.road.tollbooth.client.api.PatchOperation;

public class HiveStatusPatchBuilderTest {

  @Test
  public void patchBuilder() {
    List<PatchOperation> operations = HiveStatusPatchBuilder
        .patchBuilder()
        .set(AgentProperty.TRUCK_PARK_STATE, "bar")
        .build();

    assertThat(operations.size(), is(1));
    PatchOperation operation = operations.get(0);
    assertThat(operation.getOperation(), is(Operation.REPLACE));
    assertThat(operation.getPath(), is("/destinations/hive/status/truckParkState"));
    assertThat(operation.getValue(), is("bar"));
  }

}
