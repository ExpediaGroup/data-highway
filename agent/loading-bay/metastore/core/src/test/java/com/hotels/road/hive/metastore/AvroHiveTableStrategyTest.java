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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.maven.version.DataHighwayVersion;

@RunWith(MockitoJUnitRunner.class)
public class AvroHiveTableStrategyTest {
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String PARTITION_COLUMN = "partition_column";
  private static final String LOCATION = "location";

  private @Mock SchemaUriResolver uriResolver;
  private @Mock Clock clock;

  Schema schema1 = SchemaBuilder
      .record("my_schema")
      .fields()
      .name("my_field")
      .type()
      .booleanType()
      .noDefault()
      .endRecord();
  Schema schema2 = SchemaBuilder
      .record("my_schema")
      .fields()
      .name("my_field")
      .type()
      .booleanType()
      .booleanDefault(true)
      .endRecord();

  private HiveTableStrategy underTest;

  @Before
  public void injectMocks() {
    underTest = new AvroHiveTableStrategy(uriResolver, clock);
  }

  @Test
  public void newHiveTable() throws URISyntaxException {
    when(uriResolver.resolve(schema1, TABLE, 1))
        .thenReturn(new URI("https://s3.amazonaws.com/road-schema-bucket/roads/table/schemas/1/table_v1.avsc"));
    doReturn(Instant.ofEpochSecond(1526462225L)).when(clock).instant();

    Table result = underTest.newHiveTable(DATABASE, TABLE, PARTITION_COLUMN, LOCATION, schema1, 1);

    assertThat(result.getDbName(), is(DATABASE));
    assertThat(result.getTableName(), is(TABLE));
    assertThat(result.getTableType(), is(TableType.EXTERNAL_TABLE.toString()));
    Map<String, String> parameters = result.getParameters();
    assertThat(parameters.get("EXTERNAL"), is("TRUE"));
    assertThat(parameters.get("data-highway.version"), is(DataHighwayVersion.VERSION));
    assertThat(parameters.get("data-highway.last-revision"), is("2018-05-16T09:17:05Z"));
    assertThat(parameters.get(AvroHiveTableStrategy.AVRO_SCHEMA_URL),
        is("https://s3.amazonaws.com/road-schema-bucket/roads/table/schemas/1/table_v1.avsc"));
    assertThat(parameters.get(AvroHiveTableStrategy.AVRO_SCHEMA_VERSION), is("1"));
    List<FieldSchema> partitionKeys = result.getPartitionKeys();
    assertThat(partitionKeys.size(), is(1));
    assertThat(partitionKeys.get(0), is(new FieldSchema(PARTITION_COLUMN, "string", null)));
    StorageDescriptor storageDescriptor = result.getSd();
    assertThat(storageDescriptor.getInputFormat(), is(AvroStorageDescriptorFactory.AVRO_INPUT_FORMAT));
    assertThat(storageDescriptor.getOutputFormat(), is(AvroStorageDescriptorFactory.AVRO_OUTPUT_FORMAT));
    assertThat(storageDescriptor.getLocation(), is(LOCATION));
    assertThat(storageDescriptor.getCols().size(), is(0));
    SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
    assertThat(serdeInfo.getSerializationLib(), is(AvroStorageDescriptorFactory.AVRO_SERDE));
  }

  @Test
  public void alterHiveTable() throws URISyntaxException {
    when(uriResolver.resolve(schema1, TABLE, 1))
        .thenReturn(new URI("https://s3.amazonaws.com/road-schema-bucket/roads/table/schemas/1/table_v1.avsc"));
    when(uriResolver.resolve(schema2, TABLE, 2))
        .thenReturn(new URI("https://s3.amazonaws.com/road-schema-bucket/roads/table/schemas/2/table_v2.avsc"));
    doReturn(Instant.ofEpochSecond(1526462225L)).when(clock).instant();

    Table table = underTest.newHiveTable(DATABASE, TABLE, PARTITION_COLUMN, LOCATION, schema1, 1);

    Table result = underTest.alterHiveTable(table, schema2, 2);

    assertThat(result.getDbName(), is(DATABASE));
    assertThat(result.getTableName(), is(TABLE));
    assertThat(result.getTableType(), is(TableType.EXTERNAL_TABLE.toString()));
    Map<String, String> parameters = result.getParameters();
    assertThat(parameters.get("EXTERNAL"), is("TRUE"));
    assertThat(parameters.get("data-highway.version"), is(DataHighwayVersion.VERSION));
    assertThat(parameters.get("data-highway.last-revision"), is("2018-05-16T09:17:05Z"));
    assertThat(parameters.get(AvroHiveTableStrategy.AVRO_SCHEMA_URL),
        is("https://s3.amazonaws.com/road-schema-bucket/roads/table/schemas/2/table_v2.avsc"));
    assertThat(parameters.get(AvroHiveTableStrategy.AVRO_SCHEMA_VERSION), is("2"));
    List<FieldSchema> partitionKeys = result.getPartitionKeys();
    assertThat(partitionKeys.size(), is(1));
    assertThat(partitionKeys.get(0), is(new FieldSchema(PARTITION_COLUMN, "string", null)));
    StorageDescriptor storageDescriptor = result.getSd();
    assertThat(storageDescriptor.getInputFormat(), is(AvroStorageDescriptorFactory.AVRO_INPUT_FORMAT));
    assertThat(storageDescriptor.getOutputFormat(), is(AvroStorageDescriptorFactory.AVRO_OUTPUT_FORMAT));
    assertThat(storageDescriptor.getLocation(), is(LOCATION));
    assertThat(storageDescriptor.getCols().size(), is(0));
    SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
    assertThat(serdeInfo.getSerializationLib(), is(AvroStorageDescriptorFactory.AVRO_SERDE));
  }

  @Test
  public void getSchemaVersion() {
    Table table = new Table();
    table.putToParameters(AvroHiveTableStrategy.AVRO_SCHEMA_VERSION, Integer.toString(1));
    int result = underTest.getSchemaVersion(table);
    assertThat(result, is(1));
  }

}
