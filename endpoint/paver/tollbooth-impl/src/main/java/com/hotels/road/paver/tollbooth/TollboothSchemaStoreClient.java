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
package com.hotels.road.paver.tollbooth;

import static java.lang.String.format;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import static com.google.common.base.Preconditions.checkArgument;

import static com.hotels.road.schema.validation.DataHighwaySchemaValidator.UnionRule.ALLOW_NON_NULLABLE_UNIONS;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.jasvorno.schema.SchemaValidationException;
import com.hotels.road.exception.InvalidSchemaException;
import com.hotels.road.exception.InvalidSchemaVersionException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.paver.api.SchemaStoreClient;
import com.hotels.road.schema.chronology.SchemaCompatibility;
import com.hotels.road.schema.chronology.SchemaCompatibilityException;
import com.hotels.road.schema.gdpr.PiiSchemaEvolutionValidator;
import com.hotels.road.schema.validation.DataHighwaySchemaValidator;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@Component
public class TollboothSchemaStoreClient implements SchemaStoreClient {

  private static final Comparator<SchemaVersion> VERSION_COMPARATOR = (s1, s2) -> schemaVersion(s1) - schemaVersion(s2);
  private static final Predicate<SchemaVersion> NOT_DELETED = s -> !s.isDeleted();

  private static int schemaVersion(SchemaVersion schemaVersion) {
    return Optional.ofNullable(schemaVersion).map(SchemaVersion::getVersion).orElse(0);
  }

  private final Map<String, Road> store;
  private final PatchSetEmitter modificationEmitter;

  @Autowired
  public TollboothSchemaStoreClient(@Value("#{store}") Map<String, Road> store, PatchSetEmitter modificationEmitter) {
    this.store = store;
    this.modificationEmitter = modificationEmitter;
  }

  @Override
  public Optional<SchemaVersion> getSchema(String name, int version) throws ServiceException, UnknownRoadException {
    checkNotBlank(name, "name");
    checkArgument(version > 0, format("Schema version must not be < 1, was: %s", version));
    return Optional.ofNullable(getRoadSchemas(name).get(version));
  }

  @Override
  public Optional<SchemaVersion> getActiveSchema(String name, int version)
    throws ServiceException, UnknownRoadException {
    return getSchema(name, version).filter(NOT_DELETED);
  }

  @Override
  public Optional<SchemaVersion> getLatestActiveSchema(String name) throws ServiceException, UnknownRoadException {
    checkNotBlank(name, "name");
    return getRoadSchemas(name).values().stream().filter(NOT_DELETED).max(VERSION_COMPARATOR);
  }

  private Map<Integer, SchemaVersion> getRoadSchemas(String name) throws UnknownRoadException {
    return getRoad(name).getSchemas();
  }

  @Override
  public SchemaVersion registerSchema(String name, Schema schema)
    throws InvalidSchemaException, ServiceException, UnknownRoadException {
    return registerSchema(name, schema, OptionalInt.empty());
  }

  @Override
  public SchemaVersion registerSchema(String name, Schema schema, int version)
    throws InvalidSchemaException, ServiceException, UnknownRoadException, InvalidSchemaVersionException {
    return registerSchema(name, schema, OptionalInt.of(version));
  }

  private SchemaVersion registerSchema(String name, Schema schema, OptionalInt optionalVersion)
    throws InvalidSchemaException, ServiceException, UnknownRoadException, InvalidSchemaVersionException {
    checkNotBlank(name, "name");

    try {
      DataHighwaySchemaValidator.validate(schema, ALLOW_NON_NULLABLE_UNIONS);
    } catch (IllegalArgumentException | SchemaValidationException e) {
      throw new InvalidSchemaException(e.getMessage());
    }

    Road road = getRoad(name);
    Collection<SchemaVersion> allSchemas = road.getSchemas().values();
    Map<Integer, Schema> activeSchemas = allSchemas.stream().filter(NOT_DELETED).sorted(VERSION_COMPARATOR).collect(
        Collectors.toMap(SchemaVersion::getVersion, SchemaVersion::getSchema, duplicateVersionChecker(),
            () -> new TreeMap<>()));

    try {
      SchemaCompatibility compatibilityMode = SchemaCompatibility.valueOf(road.getCompatibilityMode());
      compatibilityMode.validate(schema, activeSchemas);
    } catch (SchemaCompatibilityException e) {
      throw new InvalidSchemaException(e.getMessage());
    }

    Optional<Schema> currentSchema = SchemaVersion.latest(allSchemas).map(SchemaVersion::getSchema);
    PiiSchemaEvolutionValidator.validate(schema, currentSchema);

    int currentMaxVersion = allSchemas.stream().mapToInt(SchemaVersion::getVersion).max().orElse(0);
    int version = optionalVersion.orElseGet(() -> currentMaxVersion + 1);

    if (version <= currentMaxVersion) {
      throw new InvalidSchemaVersionException(
          String.format("The requested version (%d) must be greater than the largest registered version (%d).", version,
              currentMaxVersion));
    }

    SchemaVersion schemaVersion = new SchemaVersion(schema, version, false);

    modificationEmitter
        .emit(new PatchSet(name, Collections.singletonList(PatchOperation.add("/schemas/" + version, schemaVersion))));
    return schemaVersion;
  }

  @Override
  public Map<Integer, SchemaVersion> getAllSchemaVersions(String name)
    throws ServiceException, IllegalArgumentException, UnknownRoadException {
    checkNotBlank(name, "name");
    return Collections.unmodifiableMap(getRoadSchemas(name));
  }

  @Override
  public Map<Integer, SchemaVersion> getActiveSchemaVersions(String name)
    throws ServiceException, IllegalArgumentException, UnknownRoadException {
    return getRoadSchemas(name).entrySet().stream().filter(s -> NOT_DELETED.test(s.getValue())).collect(
        Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  private static void checkNotBlank(String value, String attribute) {
    checkArgument(isNotBlank(value), format("Road %s must not be blank.", attribute));
  }

  @Override
  public void deleteSchemaVersion(String name, int version) throws UnknownRoadException {
    Optional.of(getRoad(name)).map(Road::getSchemas).map(m -> m.get(version)).ifPresent(markSchemaAsDeleted(name));
  }

  private Consumer<? super SchemaVersion> markSchemaAsDeleted(String name) {
    return s -> modificationEmitter.emit(new PatchSet(name,
        Collections.singletonList(PatchOperation.replace("/schemas/" + s.getVersion() + "/deleted", true))));
  }

  private Road getRoad(String name) throws UnknownRoadException {
    Road road = store.get(name);
    if(road == null || road.isDeleted()) {
      road = null;
    }
    return Optional.ofNullable(road).orElseThrow(() -> new UnknownRoadException(name));
  }

  private static <T> BinaryOperator<T> duplicateVersionChecker() {
    return (u, v) -> {
      throw new IllegalStateException(String.format("Duplicate key %s", u));
    };
  }

}
