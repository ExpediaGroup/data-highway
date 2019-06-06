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
package com.hotels.road.hive.metastore;

import java.net.URI;

import org.apache.avro.Schema;

/**
 * Abstraction for resolving {@link URI URIs} to {@link Schema Schemas} so that they can be retrieved by Hive's
 * {@code AvroSerDe} using the {@code avro.schema.url} table/partition parameter. Note that implementations may first
 * have to store the {@link Schema} resource in some system before a {@link URI} can be provided.
 */
public interface SchemaUriResolver {

  URI resolve(Schema schema, String road, int version);

}
