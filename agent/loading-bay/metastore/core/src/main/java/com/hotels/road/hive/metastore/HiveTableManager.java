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

import static java.util.Collections.singletonList;

import static org.apache.hadoop.hive.metastore.api.HiveObjectType.TABLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;

import java.net.URI;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HiveTableManager {
  private final IMetaStoreClient metaStoreClient;
  private final HiveTableStrategy hiveTableStrategy;
  private final LocationResolver locationResolver;
  private final String databaseName;

  public boolean tableExists(String tableName) throws MetaStoreException {
    try {
      return metaStoreClient.tableExists(databaseName, tableName);
    } catch (TException e) {
      throw new MetaStoreException(e);
    }
  }

  public Table createTable(String tableName, String partitionColumnName, Schema schema, int version, String owner)
    throws MetaStoreException {
    try {
      URI location = locationResolver.resolveLocation(databaseName + "/" + tableName);
      Table table = hiveTableStrategy.newHiveTable(databaseName, tableName, partitionColumnName, location.toString(),
          schema, version);
      table.setOwner(owner);
      metaStoreClient.createTable(table);
      return table;
    } catch (TException e) {
      throw new MetaStoreException(e);
    }
  }

  public void grantPublicSelect(String tableName, String grantor) {
    HiveObjectRef hiveObject = new HiveObjectRef(TABLE, databaseName, tableName, null, null);
    PrivilegeGrantInfo grantInfo = new PrivilegeGrantInfo("SELECT", 0, grantor, ROLE, false);
    HiveObjectPrivilege privilege = new HiveObjectPrivilege(hiveObject, "public", ROLE, grantInfo);
    PrivilegeBag privilegeBag = new PrivilegeBag(singletonList(privilege));
    try {
      metaStoreClient.grant_privileges(privilegeBag);
    } catch (TException e) {
      throw new MetaStoreException(e);
    }
  }

  public void alterTable(String tableName, Schema schema, int version) throws MetaStoreException {
    try {
      Table table = metaStoreClient.getTable(databaseName, tableName);
      Table alteredTable = hiveTableStrategy.alterHiveTable(table, schema, version);
      metaStoreClient.alter_table(databaseName, tableName, alteredTable);
    } catch (TException e) {
      throw new MetaStoreException(e);
    }
  }

  public int getSchemaVersion(String tableName) throws MetaStoreException {
    try {
      Table table = metaStoreClient.getTable(databaseName, tableName);
      return hiveTableStrategy.getSchemaVersion(table);
    } catch (TException e) {
      throw new MetaStoreException(e);
    }
  }
}
