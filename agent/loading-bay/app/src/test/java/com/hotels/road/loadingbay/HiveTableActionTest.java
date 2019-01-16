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
package com.hotels.road.loadingbay;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.hotels.road.loadingbay.AgentProperty.HIVE_SCHEMA_VERSION;
import static com.hotels.road.loadingbay.AgentProperty.HIVE_TABLE_CREATED;
import static com.hotels.road.loadingbay.HiveTableAction.LOADING_BAY;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.hive.metastore.HiveTableManager;
import com.hotels.road.loadingbay.event.HiveNotificationHandler;
import com.hotels.road.loadingbay.model.Destinations;
import com.hotels.road.loadingbay.model.Hive;
import com.hotels.road.loadingbay.model.HiveRoad;
import com.hotels.road.loadingbay.model.KafkaStatus;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@RunWith(MockitoJUnitRunner.class)
public class HiveTableActionTest {

  private static final String ROAD_NAME = "road1";
  private static final String PARTITION_COLUMN_NAME = "acquisition_instant";
  private static final String BASE_LOCATION = "s3://base_location";

  @Mock
  private HiveTableManager hiveTableManager;

  private final LoadingCacheFactory loadingCacheFactory = new LoadingCacheFactory(0, SECONDS);

  private final Schema schema = SchemaBuilder.builder().stringType();
  private final SchemaVersion schemaVersion1 = new SchemaVersion(schema, 1, false);
  private final SchemaVersion schemaVersion2 = new SchemaVersion(schema, 2, false);
  private final SchemaVersion schemaVersion1Deleted = new SchemaVersion(schema, 2, true);
  private final Hive hive = Hive.builder().hivePartitionColumnName(PARTITION_COLUMN_NAME).build();
  private final Destinations destinations = Destinations.builder().hive(hive).build();
  private final KafkaStatus status = KafkaStatus.builder().build();

  private HiveTableAction underTest;

  @Mock
  private HiveNotificationHandler notificationHandler;

  @Before
  public void before() {
    underTest = new HiveTableAction(hiveTableManager, loadingCacheFactory, notificationHandler, true);
  }

  private Map<Integer, SchemaVersion> schemas(SchemaVersion... schemas) {
    return Stream.of(schemas).collect(Collectors.toMap(SchemaVersion::getVersion, Function.identity()));
  }

  @Test
  public void tableNotExists() {
    when(hiveTableManager.tableExists(ROAD_NAME)).thenReturn(false);
    when(hiveTableManager.createTable(ROAD_NAME, PARTITION_COLUMN_NAME, schemaVersion1.getSchema(), 1, LOADING_BAY))
        .thenReturn(createTableWithLocation(BASE_LOCATION));
    HiveRoad road = HiveRoad
        .builder()
        .name(ROAD_NAME)
        .schemas(schemas(schemaVersion1))
        .destinations(destinations)
        .status(status)
        .build();

    List<PatchOperation> result = underTest.checkAndApply(road);

    verify(hiveTableManager).createTable(ROAD_NAME, PARTITION_COLUMN_NAME, schemaVersion1.getSchema(), 1, LOADING_BAY);
    verify(hiveTableManager).grantPublicSelect(ROAD_NAME, LOADING_BAY);

    verify(notificationHandler).handleHiveTableCreated(ROAD_NAME, ROAD_NAME, PARTITION_COLUMN_NAME, BASE_LOCATION);

    List<PatchOperation> patches = HiveStatusPatchBuilder
        .patchBuilder()
        .set(HIVE_TABLE_CREATED, true)
        .set(HIVE_SCHEMA_VERSION, 1)
        .build();
    assertThat(result, is(patches));
  }

  @Test
  public void tableNotExists_GrantDisabled() {
    underTest = new HiveTableAction(hiveTableManager, loadingCacheFactory, notificationHandler, false);

    when(hiveTableManager.tableExists(ROAD_NAME)).thenReturn(false);
    when(hiveTableManager.createTable(ROAD_NAME, PARTITION_COLUMN_NAME, schemaVersion1.getSchema(), 1, LOADING_BAY))
        .thenReturn(createTableWithLocation(BASE_LOCATION));
    HiveRoad road = HiveRoad
        .builder()
        .name(ROAD_NAME)
        .schemas(schemas(schemaVersion1))
        .destinations(destinations)
        .status(status)
        .build();

    List<PatchOperation> result = underTest.checkAndApply(road);

    verify(hiveTableManager).createTable(ROAD_NAME, PARTITION_COLUMN_NAME, schemaVersion1.getSchema(), 1, LOADING_BAY);
    verify(hiveTableManager, never()).grantPublicSelect(ROAD_NAME, LOADING_BAY);

    verify(notificationHandler).handleHiveTableCreated(ROAD_NAME, ROAD_NAME, PARTITION_COLUMN_NAME, BASE_LOCATION);

    List<PatchOperation> patches = HiveStatusPatchBuilder
        .patchBuilder()
        .set(HIVE_TABLE_CREATED, true)
        .set(HIVE_SCHEMA_VERSION, 1)
        .build();
    assertThat(result, is(patches));
  }

  private Table createTableWithLocation(String baseLocation) {
    Table table = new Table();
    table.setSd(new StorageDescriptor());
    table.getSd().setLocation(baseLocation);
    return table;
  }

  @Test
  public void tableNotExists_MultipleSchemas() {
    when(hiveTableManager.tableExists(ROAD_NAME)).thenReturn(false);
    when(hiveTableManager.createTable(ROAD_NAME, PARTITION_COLUMN_NAME, schema, 2, LOADING_BAY))
        .thenReturn(createTableWithLocation(BASE_LOCATION));

    HiveRoad road = HiveRoad
        .builder()
        .name(ROAD_NAME)
        .schemas(schemas(schemaVersion1, schemaVersion2))
        .destinations(destinations)
        .status(status)
        .build();

    List<PatchOperation> result = underTest.checkAndApply(road);

    verify(hiveTableManager).createTable(ROAD_NAME, PARTITION_COLUMN_NAME, schema, 2, LOADING_BAY);

    List<PatchOperation> patches = HiveStatusPatchBuilder
        .patchBuilder()
        .set(HIVE_TABLE_CREATED, true)
        .set(HIVE_SCHEMA_VERSION, 2)
        .build();
    assertThat(result, is(patches));

    verify(notificationHandler).handleHiveTableCreated(ROAD_NAME, ROAD_NAME, PARTITION_COLUMN_NAME, BASE_LOCATION);
  }

  @Test
  public void schemaChanged() {
    when(hiveTableManager.tableExists(ROAD_NAME)).thenReturn(true);
    when(hiveTableManager.getSchemaVersion(ROAD_NAME)).thenReturn(1);

    HiveRoad road = HiveRoad
        .builder()
        .name(ROAD_NAME)
        .schemas(schemas(schemaVersion1, schemaVersion2))
        .destinations(destinations)
        .status(status)
        .build();

    List<PatchOperation> result = underTest.checkAndApply(road);

    verify(hiveTableManager).alterTable(ROAD_NAME, schema, 2);

    List<PatchOperation> patches = HiveStatusPatchBuilder.patchBuilder().set(HIVE_SCHEMA_VERSION, 2).build();
    assertThat(result, is(patches));
    verifyNoMoreInteractions(notificationHandler);
  }

  @Test
  public void tableExistsAndSchemaNotChanged() {
    when(hiveTableManager.tableExists(ROAD_NAME)).thenReturn(true);
    when(hiveTableManager.getSchemaVersion(ROAD_NAME)).thenReturn(1);

    HiveRoad road = HiveRoad
        .builder()
        .name(ROAD_NAME)
        .schemas(schemas(schemaVersion1))
        .destinations(destinations)
        .status(status)
        .build();

    List<PatchOperation> result = underTest.checkAndApply(road);

    List<PatchOperation> patches = Collections.emptyList();
    assertThat(result, is(patches));
    verifyNoMoreInteractions(notificationHandler);
  }

  @Test(expected = NoActiveSchemaException.class)
  public void noSchema() {
    HiveRoad road = HiveRoad
        .builder()
        .name(ROAD_NAME)
        .schemas(schemas(schemaVersion1Deleted))
        .destinations(destinations)
        .status(status)
        .build();

    underTest.checkAndApply(road);
    verifyNoMoreInteractions(notificationHandler);
  }
}
