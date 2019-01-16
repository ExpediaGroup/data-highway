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
package com.hotels.road.model.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Optional;

import org.junit.Test;

public class SchemaVersionTest {

  @Test
  public void latest() {
    SchemaVersion schemaVersion1 = new SchemaVersion(null, 1, false);
    SchemaVersion schemaVersion2 = new SchemaVersion(null, 2, false);

    Optional<SchemaVersion> result = SchemaVersion.latest(Arrays.asList(schemaVersion1, schemaVersion2));

    assertThat(result.isPresent(), is(true));
    assertThat(result.get().getVersion(), is(2));
  }

  @Test
  public void latestDeleted() {
    SchemaVersion schemaVersion1 = new SchemaVersion(null, 1, false);
    SchemaVersion schemaVersion2 = new SchemaVersion(null, 2, true);

    Optional<SchemaVersion> result = SchemaVersion.latest(Arrays.asList(schemaVersion1, schemaVersion2));

    assertThat(result.isPresent(), is(true));
    assertThat(result.get().getVersion(), is(1));
  }

  @Test
  public void latestAllDeleted() {
    SchemaVersion schemaVersion1 = new SchemaVersion(null, 1, true);
    SchemaVersion schemaVersion2 = new SchemaVersion(null, 2, true);

    Optional<SchemaVersion> result = SchemaVersion.latest(Arrays.asList(schemaVersion1, schemaVersion2));

    assertThat(result.isPresent(), is(false));
  }

  @Test
  public void version() {
    SchemaVersion schemaVersion1 = new SchemaVersion(null, 1, false);
    SchemaVersion schemaVersion2 = new SchemaVersion(null, 2, false);

    Optional<SchemaVersion> result = SchemaVersion.version(Arrays.asList(schemaVersion1, schemaVersion2), 1);

    assertThat(result.isPresent(), is(true));
    assertThat(result.get().getVersion(), is(1));
  }

  @Test
  public void versionNotFound() {
    SchemaVersion schemaVersion1 = new SchemaVersion(null, 1, false);
    SchemaVersion schemaVersion2 = new SchemaVersion(null, 2, false);

    Optional<SchemaVersion> result = SchemaVersion.version(Arrays.asList(schemaVersion1, schemaVersion2), 3);

    assertThat(result.isPresent(), is(false));
  }
}
