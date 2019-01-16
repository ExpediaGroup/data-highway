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

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.tollbooth.client.api.PatchOperation;

public class PartitionPathPatchMappingTest extends AbstractPatchMappingTest {
  private final PartitionPathPatchMapping mapping = new PartitionPathPatchMapping();

  @Before
  public void addSchemas() {
    Schema schema1 = SchemaBuilder.record("a").fields().requiredString("foo").endRecord();
    Schema schema2 = SchemaBuilder
        .record("a")
        .fields()
        .requiredString("foo")
        .nullableString("bar", "DEFAULT")
        .endRecord();
    Map<Integer, SchemaVersion> schemas = ImmutableMap.of(1, new SchemaVersion(schema1, 1, false), 2,
        new SchemaVersion(schema2, 2, false));
    road.setSchemas(schemas);
  }

  @Test
  public void checkPath() throws Exception {
    assertThat(mapping.getPath(), is("/partitionPath"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void partitionPath_cannot_be_set_to_null() throws Exception {
    mapping.convertOperation(road, PatchOperation.replace("/partitionPath", null));
  }

  @Test
  public void partitionPath_valid_in_latest_schema() throws Exception {
    mapping.convertOperation(road, PatchOperation.replace("/partitionPath", "$.bar"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void partitionPath_not_valid_in_latest_schema() throws Exception {
    mapping.convertOperation(road, PatchOperation.replace("/partitionPath", "$.baz"));
  }

  @Test
  public void add_converts_to_replace() throws Exception {
    PatchOperation operation = mapping.convertOperation(road, PatchOperation.add("/partitionPath", "$.foo"));

    assertThat(operation, is(PatchOperation.replace("/partitionPath", "$.foo")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void remove_patch_fails() throws Exception {
    mapping.convertOperation(road, PatchOperation.remove("/partitionPath"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void setting_to_a_non_string_fails() throws Exception {
    mapping.convertOperation(road, PatchOperation.add("/partitionPath", false));
  }

}
