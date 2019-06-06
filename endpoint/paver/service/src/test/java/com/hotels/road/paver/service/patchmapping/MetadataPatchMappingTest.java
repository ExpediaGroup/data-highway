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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.hotels.road.tollbooth.client.api.PatchOperation;

public class MetadataPatchMappingTest extends AbstractPatchMappingTest {
  private final MetadataPatchMapping mapping = new MetadataPatchMapping();

  @Test
  public void checkPath() throws Exception {
    assertThat(mapping.getPath(), is("/metadata"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void metadata_cannot_be_set_to_null() throws Exception {
    mapping.convertOperation(road, PatchOperation.replace("/metadata", null));
  }

  @Test
  public void add_converts_to_replace() throws Exception {
    PatchOperation operation = mapping.convertOperation(road, PatchOperation.add("/metadata", emptyMap()));

    assertThat(operation, is(PatchOperation.replace("/metadata", emptyMap())));
  }

  @Test(expected = IllegalArgumentException.class)
  public void remove_patch_fails() throws Exception {
    mapping.convertOperation(road, PatchOperation.remove("/metadata"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void setting_to_a_non_map_fails() throws Exception {
    mapping.convertOperation(road, PatchOperation.add("/metadata", emptyList()));
  }
}
