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
package com.hotels.road.loadingbay;

import static com.hotels.road.loadingbay.AgentProperty.HIVE_SCHEMA_VERSION;
import static com.hotels.road.loadingbay.AgentProperty.HIVE_TABLE_CREATED;
import static com.hotels.road.loadingbay.LanderTaskRunner.ACQUISITION_INSTANT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Ordering;

import com.hotels.road.hive.metastore.HiveTableManager;
import com.hotels.road.loadingbay.event.HiveNotificationHandler;
import com.hotels.road.loadingbay.model.HiveRoad;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@Slf4j
@Component
public class HiveTableAction {
  static final String LOADING_BAY = "datahighway-loading-bay";

  private final HiveTableManager hiveTableManager;

  private final LoadingCache<String, Boolean> tableExistsLookup;
  private final Predicate<String> tableExists;

  private final LoadingCache<String, Integer> schemaVersionLookup;
  private final BiPredicate<String, Integer> schemaVersionChanged;

  private final HiveNotificationHandler hiveNotificationHandler;
  private final boolean grantPublicSelect;

  @Autowired
  public HiveTableAction(
      HiveTableManager hiveTableManager,
      LoadingCacheFactory loadingCacheFactory,
      HiveNotificationHandler hiveNotificationHandler,
      @Value("${hive.grantPublicSelect:false}") boolean grantPublicSelect) {
    this.hiveTableManager = hiveTableManager;
    this.hiveNotificationHandler = hiveNotificationHandler;
    this.grantPublicSelect = grantPublicSelect;

    tableExistsLookup = loadingCacheFactory.newInstance(name -> hiveTableManager.tableExists(name));
    tableExists = name -> tableExistsLookup.getUnchecked(name);

    schemaVersionLookup = loadingCacheFactory.newInstance(name -> hiveTableManager.getSchemaVersion(name));
    schemaVersionChanged = (name, version) -> !schemaVersionLookup.getUnchecked(name).equals(version);
  }

  /**
   * Checks the state of the Hive table, creating and updating if required. Returns the schema version applied.
   */
  public List<PatchOperation> checkAndApply(HiveRoad road) {
    List<PatchOperation> patches = new ArrayList<>();

    String name = road.getName();
    SchemaVersion latestSchemaVersion = road
        .getSchemas()
        .entrySet()
        .stream()
        .map(Entry::getValue)
        .filter(s -> !s.isDeleted())
        .max(Ordering.natural().onResultOf(SchemaVersion::getVersion))
        .orElseThrow(() -> new NoActiveSchemaException());

    Schema schema = latestSchemaVersion.getSchema();
    int version = latestSchemaVersion.getVersion();

    if (!tableExists.test(name)) {
      patches.addAll(createTable(name, schema, version));
    } else if (schemaVersionChanged.test(name, version)) {
      patches.addAll(updateSchema(name, schema, version));
    } else {
      log.debug("Nothing to do for road {}", road.getName());
    }

    return patches;
  }

  private List<PatchOperation> createTable(String name, Schema schema, int version) {
    log.info("Creating table for road '{}'", name);
    Table table = hiveTableManager.createTable(name, ACQUISITION_INSTANT, schema, version, LOADING_BAY);
    if (grantPublicSelect) {
      hiveTableManager.grantPublicSelect(name, LOADING_BAY);
    }
    String tableLocation = Optional.ofNullable(table).map(Table::getSd).map(StorageDescriptor::getLocation).orElse(
        null);
    log.info("Table created for road '{}'", name);
    tableExistsLookup.invalidate(name);
    hiveNotificationHandler.handleHiveTableCreated(name, name, ACQUISITION_INSTANT, tableLocation);

    return HiveStatusPatchBuilder
        .patchBuilder()
        .set(HIVE_TABLE_CREATED, true)
        .set(HIVE_SCHEMA_VERSION, version)
        .build();
  }

  private List<PatchOperation> updateSchema(String name, Schema schema, int version) {
    log.info("Updating table for road '{}' to schema version {}", name, version);
    hiveTableManager.alterTable(name, schema, version);
    log.info("Updated table for road '{}' to schema version {}", name, version);
    schemaVersionLookup.invalidate(name);
    return HiveStatusPatchBuilder.patchBuilder().set(HIVE_SCHEMA_VERSION, version).build();
  }

}
