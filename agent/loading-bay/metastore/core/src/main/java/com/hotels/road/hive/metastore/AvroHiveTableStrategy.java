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
package com.hotels.road.hive.metastore;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

import java.net.URI;
import java.time.Clock;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.RequiredArgsConstructor;

import com.hotels.road.maven.version.DataHighwayVersion;

@RequiredArgsConstructor
public class AvroHiveTableStrategy implements HiveTableStrategy {
  static final String AVRO_SCHEMA_URL = "avro.schema.url";
  static final String AVRO_SCHEMA_VERSION = "avro.schema.version";

  private final SchemaUriResolver uriResolver;
  private final Clock clock;

  @Override
  public Table newHiveTable(
      String databaseName,
      String tableName,
      String partitionColumnName,
      String location,
      Schema schema,
      int version) {

    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);

    table.setTableType(TableType.EXTERNAL_TABLE.toString());
    table.putToParameters("EXTERNAL", "TRUE");
    addRoadAnnotations(table);

    URI schemaUri = uriResolver.resolve(schema, table.getTableName(), version);
    table.putToParameters(AVRO_SCHEMA_URL, schemaUri.toString());
    table.putToParameters(AVRO_SCHEMA_VERSION, Integer.toString(version));
    table.setPartitionKeys(Arrays.asList(new FieldSchema(partitionColumnName, "string", null)));

    table.setSd(AvroStorageDescriptorFactory.create(location));

    return table;
  }

  @Override
  public Table alterHiveTable(Table table, Schema schema, int version) {
    Table alteredTable = new Table(table);
    addRoadAnnotations(alteredTable);
    URI schemaUri = uriResolver.resolve(schema, table.getTableName(), version);
    alteredTable.putToParameters(AVRO_SCHEMA_URL, schemaUri.toString());
    alteredTable.putToParameters(AVRO_SCHEMA_VERSION, Integer.toString(version));
    return alteredTable;
  }

  private void addRoadAnnotations(Table table) {
    table.putToParameters("data-highway.version", DataHighwayVersion.VERSION);
    table.putToParameters("data-highway.last-revision", ISO_OFFSET_DATE_TIME.withZone(UTC).format(clock.instant()));
  }

  @Override
  public int getSchemaVersion(Table table) {
    return Integer.parseInt(table.getParameters().get(AVRO_SCHEMA_VERSION));
  }
}
