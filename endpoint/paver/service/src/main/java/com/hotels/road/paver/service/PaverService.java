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
package com.hotels.road.paver.service;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.avro.Schema;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.InvalidKeyPathException;
import com.hotels.road.exception.InvalidSchemaVersionException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.paver.service.exception.NoSuchSchemaException;
import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.rest.model.RoadModel;
import com.hotels.road.tollbooth.client.api.PatchOperation;

public interface PaverService {
  SortedSet<String> getRoadNames();

  void createRoad(BasicRoadModel road) throws AlreadyExistsException;

  RoadModel getRoad(String name) throws UnknownRoadException;

  Map<Integer, Schema> getActiveSchemas(String name) throws UnknownRoadException;

  SchemaVersion getActiveSchema(String name, int version) throws UnknownRoadException, NoSuchSchemaException;

  SchemaVersion getLatestActiveSchema(String name) throws UnknownRoadException, NoSuchSchemaException;

  SchemaVersion addSchema(String name, Schema schema) throws UnknownRoadException, InvalidKeyPathException;

  SchemaVersion addSchema(String name, Schema schema, int version)
    throws UnknownRoadException, InvalidKeyPathException, InvalidSchemaVersionException;

  void applyPatch(String name, List<PatchOperation> patchSet) throws UnknownRoadException;

  void deleteSchemaVersion(String name, int version) throws UnknownRoadException;

  void deleteRoad(String name) throws UnknownRoadException;
}
