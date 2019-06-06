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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.notification.NotificationHandler;
import com.hotels.road.notification.model.DataHighwayNotification.RoadNotificationType;
import com.hotels.road.notification.model.HivePartitionCreated;
import com.hotels.road.notification.model.HiveTableCreated;

@RunWith(MockitoJUnitRunner.class)
public class HiveNotificationHandlerTest {

  private static final String ROAD_NAME = "ROAD_NAME";
  private static final String PARTITION_SPEC = "PARTITION_SPEC";
  private static final String PARTITION_COLUMN_NAME = "PARTITION_COLUMN_NAME";
  private static final String HIVE_METASTORE_URIS = "HIVE_METASTORE_URIS";
  private static final String DATABASE_NAME = "DATABASE_NAME";
  private static final String LOCATION_URI = "LOCATION_URI";
  private static final String TABLE_NAME = "TABLE_NAME";
  private static final String BASE_LOCATION = "s3://base_table_location";

  @Mock
  private NotificationHandler notificationHandler;
  @Mock
  private Partition partition;
  @Mock
  private StorageDescriptor sd;
  @Captor
  private ArgumentCaptor<HivePartitionCreated> hivePartitionCreatedCaptor;
  @Captor
  private ArgumentCaptor<HiveTableCreated> hiveTableCreatedCaptor;

  private HiveNotificationHandler underTest;

  @Before
  public void setUp() {
    underTest = new HiveNotificationHandler(notificationHandler, HIVE_METASTORE_URIS, DATABASE_NAME);
  }

  @Test
  public void hivePartitionCreated() {
    when(partition.getDbName()).thenReturn(DATABASE_NAME);
    when(partition.getTableName()).thenReturn(TABLE_NAME);
    when(partition.getSd()).thenReturn(sd);
    when(sd.getLocation()).thenReturn(LOCATION_URI);

    doNothing().when(notificationHandler).send(hivePartitionCreatedCaptor.capture());

    underTest = new HiveNotificationHandler(notificationHandler, HIVE_METASTORE_URIS, DATABASE_NAME);
    underTest.handlePartitionCreated(ROAD_NAME, partition, PARTITION_SPEC, 1L);

    HivePartitionCreated notification = hivePartitionCreatedCaptor.getValue();
    assertThat(notification.getDatabaseName(), is(DATABASE_NAME));
    assertThat(notification.getLocationUri(), is(LOCATION_URI));
    assertThat(notification.getMetastoreUris(), is(HIVE_METASTORE_URIS));
    assertThat(notification.getPartitionSpec(), is(PARTITION_SPEC));
    assertThat(notification.getRecordCount(), is(1L));

    assertThat(notification.getProtocolVersion(), is("1.0"));
    assertThat(notification.getType(), is(RoadNotificationType.HIVE_PARTITION_CREATED));
    assertThat(notification.getRoadName(), is(ROAD_NAME));
    assertThat(notification.getTableName(), is(TABLE_NAME));
  }

  @Test
  public void hiveTableCreated() {
    underTest.handleHiveTableCreated(ROAD_NAME, TABLE_NAME, PARTITION_COLUMN_NAME, BASE_LOCATION);

    verify(notificationHandler).send(hiveTableCreatedCaptor.capture());
    HiveTableCreated notification = hiveTableCreatedCaptor.getValue();
    assertThat(notification.getDatabaseName(), is(DATABASE_NAME));
    assertThat(notification.getPartitionColumnName(), is(PARTITION_COLUMN_NAME));
    assertThat(notification.getBaseLocation(),
        is(BASE_LOCATION));
    assertThat(notification.getMetastoreUris(), is(HIVE_METASTORE_URIS));

    assertThat(notification.getProtocolVersion(), is("1.0"));
    assertThat(notification.getType(), is(RoadNotificationType.HIVE_TABLE_CREATED));
    assertThat(notification.getRoadName(), is(ROAD_NAME));
    assertThat(notification.getTableName(), is(TABLE_NAME));
  }

}
