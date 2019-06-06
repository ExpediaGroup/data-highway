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
package com.hotels.road.offramp.service;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.List;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.hotels.road.pii.PiiReplacer;
import com.hotels.road.schema.SchemaTraverser;
import com.hotels.road.schema.gdpr.PiiPathVisitor;

@Component
class PiiDataReplacer {
  private final JsonNodeTransformer transformer;
  private final LoadingCache<Schema, List<String>> piiPathsCache;

  PiiDataReplacer(PiiReplacer replacer) {
    transformer = new JsonNodeTransformer(new JsonNodePiiReplacer(replacer));
    piiPathsCache = CacheBuilder.newBuilder().concurrencyLevel(1).expireAfterAccess(1, MINUTES).build(
        CacheLoader.from(schema -> SchemaTraverser.traverse(schema, new PiiPathVisitor())));
  }

  JsonNode replace(Schema schema, JsonNode message) {
    List<String> piiPaths = piiPathsCache.getUnchecked(schema);
    return transformer.transform(message, piiPaths);
  }
}
