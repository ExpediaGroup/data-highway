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
package com.hotels.road.paver.service.patchmapping;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.tollbooth.client.api.PatchOperation;

@RunWith(MockitoJUnitRunner.class)
public class EnabledPatchMappingTest extends AbstractPatchMappingTest {
  private final EnabledPatchMapping mapping = new EnabledPatchMapping();

  @Test
  public void checkPath() throws Exception {
    assertThat(mapping.getPath(), is("/enabled"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void enable_road_failes_when_road_is_not_ready() throws Exception {
    mapping.convertOperation(road, PatchOperation.replace("/enabled", Boolean.TRUE));
  }

  @Test
  public void enable_succeds_when_road_is_ready() throws Exception {
    road.getStatus().setTopicCreated(true);
    PatchOperation operation = mapping.convertOperation(road, PatchOperation.replace("/enabled", Boolean.TRUE));

    assertThat(operation, is(PatchOperation.replace("/enabled", Boolean.TRUE)));
  }

  @Test
  public void disable_succeeds() throws Exception {
    PatchOperation operation = mapping.convertOperation(road, PatchOperation.replace("/enabled", Boolean.FALSE));

    assertThat(operation, is(PatchOperation.replace("/enabled", Boolean.FALSE)));
  }

  @Test
  public void add_converts_to_replace() throws Exception {
    road.getStatus().setTopicCreated(true);
    PatchOperation operation = mapping.convertOperation(road, PatchOperation.add("/enabled", Boolean.TRUE));

    assertThat(operation, is(PatchOperation.replace("/enabled", Boolean.TRUE)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void remove_patch_fails() throws Exception {
    mapping.convertOperation(road, PatchOperation.remove("/enabled"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void setting_to_a_non_boolean_fails() throws Exception {
    mapping.convertOperation(road, PatchOperation.add("/enabled", "FALSE"));
  }
}
