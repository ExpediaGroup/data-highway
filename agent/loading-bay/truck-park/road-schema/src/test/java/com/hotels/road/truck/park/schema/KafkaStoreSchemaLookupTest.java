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
package com.hotels.road.truck.park.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import com.hotels.road.truck.park.decoder.SchemaLookup;
import com.hotels.road.truck.park.schema.KafkaStoreSchemaLookup.Road;
import com.hotels.road.truck.park.schema.KafkaStoreSchemaLookup.SchemaVersion;

public class KafkaStoreSchemaLookupTest {

  private static final String ROAD_NAME = "road1";
  private final Schema schema1 = SchemaBuilder.builder().stringType();
  private final SchemaVersion schemaVersion1 = new SchemaVersion(schema1, 1, false);
  private final HashMap<Integer, SchemaVersion> schemas = new HashMap<>();

  private final Map<String, Road> roads = new HashMap<>();

  private final SchemaLookup underTest = new KafkaStoreSchemaLookup(roads, ROAD_NAME);

  @Test
  public void getSchema() {
    schemas.put(1, schemaVersion1);
    Road road = new Road(schemas);
    roads.put(ROAD_NAME, road);

    Schema result = underTest.getSchema(1);

    assertThat(result, is(schema1));
  }

  @Test(expected = NullPointerException.class)
  public void roadHasNoMatchingVersion() {
    Road road = new Road(schemas);
    roads.put(ROAD_NAME, road);

    Schema result = underTest.getSchema(2);

    assertThat(result, is(schema1));
  }

}
