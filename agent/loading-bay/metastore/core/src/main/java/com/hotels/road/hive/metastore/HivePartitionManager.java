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

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.emptyMap;

import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;

import lombok.RequiredArgsConstructor;

import com.hotels.road.maven.version.DataHighwayVersion;

@RequiredArgsConstructor
public class HivePartitionManager {
  private static final String DATA_HIGHWAY_VERSION = "data-highway.version";
  private static final String DATA_HIGHWAY_LAST_REVISION = "data-highway.last-revision";
  private final IMetaStoreClient metaStoreClient;
  private final LocationResolver locationResolver;
  private final String databaseName;
  private final Clock clock;

  public Optional<Partition> addPartition(
      String tableName,
      List<String> partitionValues,
      String location,
      Map<String, String> parameters)
    throws MetaStoreException {
    try {
      URI resolvedLocation = locationResolver.resolveLocation(location);
      Partition partition = newHivePartition(tableName, partitionValues, resolvedLocation.toString(), parameters);
      partition = metaStoreClient.add_partition(partition);
      return Optional.of(partition);
    } catch (AlreadyExistsException e) {
      // This is ok, saves having to do a second call for exists
      return Optional.empty();
    } catch (TException e) {
      throw new MetaStoreException(e);
    }
  }

  public Optional<Partition> addPartition(String tableName, List<String> partitionValues, String location) {
    return addPartition(tableName, partitionValues, location, emptyMap());
  }

  public void dropPartition(String tableName, List<String> partitionValues) {
    try {
      metaStoreClient.dropPartition(databaseName, tableName, partitionValues, false);
    } catch (TException e) {
      throw new MetaStoreException(e);
    }
  }

  private Partition newHivePartition(
      String tableName,
      List<String> partitionValues,
      String location,
      Map<String, String> parameters) {
    Partition partition = new Partition();
    partition.setDbName(databaseName);
    partition.setTableName(tableName);
    partition.setValues(partitionValues);
    parameters.forEach((key, value) -> partition.putToParameters(key, value));
    partition.putToParameters(DATA_HIGHWAY_VERSION, DataHighwayVersion.VERSION);
    partition.putToParameters(DATA_HIGHWAY_LAST_REVISION, ISO_OFFSET_DATE_TIME.withZone(UTC).format(clock.instant()));
    partition.setSd(AvroStorageDescriptorFactory.create(location));
    return partition;
  }
}
