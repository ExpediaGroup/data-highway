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
package com.hotels.road.truck.park.decoder.gdpr;

import static java.util.concurrent.TimeUnit.MINUTES;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.hotels.road.schema.SchemaTraverser;
import com.hotels.road.schema.gdpr.PiiLogicalTypeVisitor;
import com.hotels.road.truck.park.decoder.SchemaLookup;

@Primary
@Component
public class PiiLogicalTypeSchemaLookup implements SchemaLookup {
  private final SchemaLookup delegate;
  private final LoadingCache<Integer, Schema> cache;

  @Autowired
  public PiiLogicalTypeSchemaLookup(SchemaLookup delegate) {
    this.delegate = delegate;
    cache = CacheBuilder.newBuilder().concurrencyLevel(1).expireAfterAccess(1, MINUTES).build(
        CacheLoader.from(this::getWithLogicalTypes));
  }

  @Override
  public Schema getSchema(int version) {
    return cache.getUnchecked(version);
  }

  private Schema getWithLogicalTypes(int version) {
    Schema schema = delegate.getSchema(version);
    SchemaTraverser.traverse(schema, new PiiLogicalTypeVisitor());
    return schema;
  }
}
