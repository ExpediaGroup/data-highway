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
package com.hotels.road.offramp.api;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;

public class SchemaProviderTest {
  private final Map<String, Road> store = new HashMap<>();
  private final SchemaProvider underTest = new SchemaProvider(store);

  @Test(expected = IllegalArgumentException.class)
  public void roadDoesNotExist() {
    store.clear();
    underTest.schema("road1", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void schemaDoesNotExist() {
    Road road = new Road();
    road.setSchemas(Collections.emptyMap());
    store.put("road1", road);
    underTest.schema("road1", 1);
  }

  @Test
  public void getSchema() {
    Road road = new Road();
    Schema schema = SchemaBuilder.builder().intType();
    road.setSchemas(Collections.singletonMap(1, new SchemaVersion(schema, 1, false)));
    store.put("road1", road);
    Schema result = underTest.schema("road1", 1);
    assertThat(result, is(schema));
  }
}
