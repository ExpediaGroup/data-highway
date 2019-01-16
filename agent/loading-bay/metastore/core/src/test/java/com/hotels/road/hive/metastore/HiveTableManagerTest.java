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

import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveTableManagerTest {
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String PARTITION_COLUMN = "partition_column";

  private URI location;

  @Mock
  private IMetaStoreClient metaStoreClient;
  @Mock
  private HiveTableStrategy hiveTableStrategy;
  @Mock
  private LocationResolver locationResolver;
  @Mock
  private Table table;
  @Mock
  private Schema schema;

  private HiveTableManager underTest;

  @Before
  public void before() throws URISyntaxException {
    location = new URI("location");
    underTest = new HiveTableManager(metaStoreClient, hiveTableStrategy, locationResolver, DATABASE);
  }

  @Test
  public void tableExists() throws Exception {
    when(metaStoreClient.tableExists(DATABASE, TABLE)).thenReturn(true);

    boolean result = underTest.tableExists(TABLE);

    verify(metaStoreClient).tableExists(DATABASE, TABLE);
    assertThat(result, is(true));
  }

  @Test(expected = MetaStoreException.class)
  public void tableExists_shouldWrapTException() throws Exception {
    doThrow(TException.class).when(metaStoreClient).tableExists(DATABASE, TABLE);

    underTest.tableExists(TABLE);
  }

  @Test
  public void createTable() throws Exception {
    when(locationResolver.resolveLocation(anyString())).thenReturn(location);
    when(
        hiveTableStrategy.newHiveTable(anyString(), anyString(), anyString(), anyString(), any(Schema.class), anyInt()))
            .thenReturn(table);

    underTest.createTable(TABLE, PARTITION_COLUMN, schema, 1, "owner");

    verify(locationResolver).resolveLocation(DATABASE + "/" + TABLE);
    verify(hiveTableStrategy).newHiveTable(DATABASE, TABLE, PARTITION_COLUMN, location.toString(), schema, 1);
    verify(table).setOwner("owner");
    verify(metaStoreClient).createTable(table);
  }

  @Test(expected = MetaStoreException.class)
  public void createTable_shouldWrapTException() throws Exception {
    when(locationResolver.resolveLocation(anyString())).thenReturn(location);
    when(
        hiveTableStrategy.newHiveTable(anyString(), anyString(), anyString(), anyString(), any(Schema.class), anyInt()))
            .thenReturn(table);
    doThrow(TException.class).when(metaStoreClient).createTable(table);

    underTest.createTable(TABLE, PARTITION_COLUMN, schema, 1, "owner");
  }

  @Test
  public void alterTable() throws Exception {
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(table);
    when(hiveTableStrategy.alterHiveTable(any(Table.class), any(Schema.class), anyInt())).thenReturn(table);

    underTest.alterTable(TABLE, schema, 1);

    verify(hiveTableStrategy).alterHiveTable(table, schema, 1);
    verify(metaStoreClient).alter_table(DATABASE, TABLE, table);
  }

  @Test(expected = MetaStoreException.class)
  public void alterTable_shouldWrapTException() throws Exception {
    doThrow(TException.class).when(metaStoreClient).alter_table(anyString(), anyString(), isNull());

    underTest.alterTable(TABLE, schema, 1);
  }

  @Test
  public void getSchemaVersion() throws Exception {
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(table);
    when(hiveTableStrategy.getSchemaVersion(table)).thenReturn(1);

    int result = underTest.getSchemaVersion(TABLE);

    assertThat(result, is(1));
  }

  @Test(expected = MetaStoreException.class)
  public void getSchemaVersion_shouldWrapTException() throws Exception {
    doThrow(TException.class).when(metaStoreClient).getTable(DATABASE, TABLE);

    underTest.getSchemaVersion(TABLE);
  }

  @Test
  public void grantPublicSelect() throws Exception {
    underTest.grantPublicSelect(TABLE, "grantor");

    ArgumentCaptor<PrivilegeBag> privilegeBagCaptor = ArgumentCaptor.forClass(PrivilegeBag.class);
    verify(metaStoreClient).grant_privileges(privilegeBagCaptor.capture());

    PrivilegeBag privilegeBag = privilegeBagCaptor.getValue();
    assertThat(privilegeBag.getPrivilegesSize(), is(1));
    HiveObjectPrivilege privilege = privilegeBag.getPrivileges().get(0);

    HiveObjectRef hiveObject = privilege.getHiveObject();
    assertThat(hiveObject.getObjectType(), is(HiveObjectType.TABLE));
    assertThat(hiveObject.getDbName(), is(DATABASE));
    assertThat(hiveObject.getObjectName(), is(TABLE));
    assertThat(hiveObject.getPartValues(), is(nullValue()));
    assertThat(hiveObject.getColumnName(), is(nullValue()));

    assertThat(privilege.getPrincipalName(), is("public"));
    assertThat(privilege.getPrincipalType(), is(ROLE));

    PrivilegeGrantInfo grantInfo = privilege.getGrantInfo();
    assertThat(grantInfo.getPrivilege(), is("SELECT"));
    assertThat(grantInfo.getCreateTime(), is(0));
    assertThat(grantInfo.getGrantor(), is("grantor"));
    assertThat(grantInfo.getGrantorType(), is(ROLE));
    assertThat(grantInfo.isGrantOption(), is(false));
  }

}
