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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.maven.version.DataHighwayVersion;

@RunWith(MockitoJUnitRunner.class)
public class HivePartitionManagerTest {
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final List<String> PARTITION_VALUES = singletonList("value");
  private static final String LOCATION = "location";

  private @Mock IMetaStoreClient metaStoreClient;
  private @Mock HiveTableStrategy hiveTableStrategy;
  private @Mock LocationResolver locationResolver;
  private @Mock Table table;
  private @Mock Schema schema;
  private @Mock Partition addedPartition;
  private @Mock Clock clock;

  private HivePartitionManager underTest;

  @Before
  public void before() {
    underTest = new HivePartitionManager(metaStoreClient, locationResolver, DATABASE, clock);
  }

  @Test
  public void addPartition() throws Exception {
    doReturn(addedPartition).when(metaStoreClient).add_partition(any());
    doReturn(URI.create("resolved/location")).when(locationResolver).resolveLocation(LOCATION);
    doReturn(Instant.ofEpochSecond(1526462225L)).when(clock).instant();

    Partition result = underTest.addPartition(TABLE, PARTITION_VALUES, LOCATION).get();

    ArgumentCaptor<Partition> captor = ArgumentCaptor.forClass(Partition.class);
    verify(metaStoreClient).add_partition(captor.capture());

    Partition partition = captor.getValue();
    assertThat(partition.getParameters().get("data-highway.version"), is(DataHighwayVersion.VERSION));
    assertThat(partition.getParameters().get("data-highway.last-revision"), is("2018-05-16T09:17:05Z"));
    assertThat(partition.getSd().getLocation(), is("resolved/location"));

    assertThat(result, is(addedPartition));
  }

  @Test(expected = MetaStoreException.class)
  public void addPartition_shouldWrapTException() throws Exception {
    doThrow(TException.class).when(metaStoreClient).add_partition(any());
    doReturn(URI.create("resolved/location")).when(locationResolver).resolveLocation(LOCATION);
    doReturn(Instant.ofEpochSecond(1526462225L)).when(clock).instant();

    underTest.addPartition(TABLE, PARTITION_VALUES, LOCATION);
  }

  @Test
  public void addPartition_shouldIgnoreAlreadyExistsException() throws Exception {
    doThrow(AlreadyExistsException.class).when(metaStoreClient).add_partition(any());
    doReturn(URI.create("resolved/location")).when(locationResolver).resolveLocation(LOCATION);
    doReturn(Instant.ofEpochSecond(1526462225L)).when(clock).instant();

    Optional<Partition> result = underTest.addPartition(TABLE, PARTITION_VALUES, LOCATION);

    assertThat(result.isPresent(), is(false));
  }

  @Test
  public void dropPartition() throws Exception {
    underTest.dropPartition(TABLE, PARTITION_VALUES);

    verify(metaStoreClient).dropPartition(DATABASE, TABLE, PARTITION_VALUES, false);
  }

  @Test(expected = MetaStoreException.class)
  public void dropPartition_shouldWrapTException() throws Exception {
    doThrow(TException.class).when(metaStoreClient).dropPartition(DATABASE, TABLE, PARTITION_VALUES, false);

    underTest.dropPartition(TABLE, PARTITION_VALUES);
  }
}
