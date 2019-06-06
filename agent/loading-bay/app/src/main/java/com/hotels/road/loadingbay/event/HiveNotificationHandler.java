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
package com.hotels.road.loadingbay.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.road.notification.NotificationHandler;
import com.hotels.road.notification.model.HivePartitionCreated;
import com.hotels.road.notification.model.HiveTableCreated;

@Component
public class HiveNotificationHandler {
  private final NotificationHandler notificationHandler;
  private final String hiveMetaStoreUris;
  private final String hiveDatabase;

  @Autowired
  public HiveNotificationHandler(
      NotificationHandler notificationHandler,
      @Value("${hive.metastore.uris}") String hiveMetaStoreUris,
      @Value("${hive.database}") String hiveDatabase) {
    this.notificationHandler = notificationHandler;
    this.hiveMetaStoreUris = hiveMetaStoreUris;
    this.hiveDatabase = hiveDatabase;
  }

  public void handlePartitionCreated(String roadName, Partition partition, String partitionSpec, long recordCount) {
    checkNotNull(partition);

    notificationHandler.send(HivePartitionCreated
        .builder()
        .roadName(roadName)
        .metastoreUris(hiveMetaStoreUris)
        .databaseName(partition.getDbName())
        .tableName(partition.getTableName())
        .partitionSpec(partitionSpec)
        .locationUri(partition.getSd().getLocation())
        .recordCount(recordCount)
        .build());
  }

  public void handleHiveTableCreated(
      String roadName,
      String tableName,
      String partitionColumnName,
      String baseLocation) {
    notificationHandler.send(HiveTableCreated
        .builder()
        .roadName(roadName)
        .tableName(tableName)
        .databaseName(hiveDatabase)
        .metastoreUris(hiveMetaStoreUris)
        .partitionColumnName(partitionColumnName)
        .baseLocation(baseLocation)
        .build());
  }
}
