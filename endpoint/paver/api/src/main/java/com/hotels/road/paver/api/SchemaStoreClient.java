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
package com.hotels.road.paver.api;

import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;

import com.hotels.road.exception.InvalidSchemaException;
import com.hotels.road.exception.InvalidSchemaVersionException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.SchemaVersion;

public interface SchemaStoreClient {

  /**
   * @param name The road name.
   * @param version The schema version.
   * @return The avro schema if it exists.
   * @throws ServiceException Any other error.
   * @throws UnknownRoadException
   */
  Optional<SchemaVersion> getSchema(String name, int version) throws ServiceException, UnknownRoadException;

  /**
   * @param name The road name.
   * @param version The schema version.
   * @return The active avro schema. {@code Optional.empty()} if schema doesn't exist or it is marked as deleted.
   * @throws ServiceException Any other error.
   * @throws UnknownRoadException
   */
  Optional<SchemaVersion> getActiveSchema(String name, int version) throws ServiceException, UnknownRoadException;

  /**
   * @param name The schema name.
   * @return The latest avro schema that is not marked as deleted.
   * @throws ServiceException Any other error.
   * @throws UnknownRoadException
   */
  Optional<SchemaVersion> getLatestActiveSchema(String name) throws ServiceException, UnknownRoadException;

  /**
   * @param name The road name.
   * @param schema The avro schema.
   * @return The schema version.
   * @throws InvalidSchemaException If the schema is invalid.
   * @throws ServiceException Any other error.
   * @throws UnknownRoadException
   */
  SchemaVersion registerSchema(String name, Schema schema)
    throws InvalidSchemaException, ServiceException, UnknownRoadException;

  /**
   * @param name The road name.
   * @param version The schema version.
   * @param schema The avro schema.
   * @return The schema version.
   * @throws InvalidSchemaException If the schema is invalid.
   * @throws ServiceException Any other error.
   * @throws UnknownRoadException
   * @throws InvalidSchemaVersionException
   */
  SchemaVersion registerSchema(String name, Schema schema, int version)
    throws InvalidSchemaException, ServiceException, UnknownRoadException, InvalidSchemaVersionException;

  /**
   * @param name The road name.
   * @param version Schema version to delete.
   * @throws UnknownRoadException if road doesn't exist
   */
  void deleteSchemaVersion(String name, int version) throws UnknownRoadException;

  /**
   * @param name The road name
   * @return A list of all historic schema in chronological order including the ones marked as deleted.
   * @throws ServiceException Any error
   * @throws UnknownRoadException if road doesn't exist
   * @throws IllegalArgumentException
   */
  Map<Integer, SchemaVersion> getAllSchemaVersions(String name)
    throws ServiceException, IllegalArgumentException, UnknownRoadException;

  /**
   * @param name
   * @return A list of all historic schema in chronological order excluding the ones marked as deleted.
   * @throws ServiceException Any error
   * @throws UnknownRoadException
   * @throws IllegalArgumentException
   */
  Map<Integer, SchemaVersion> getActiveSchemaVersions(String name)
    throws ServiceException, IllegalArgumentException, UnknownRoadException;

}
