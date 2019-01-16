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
package com.hotels.road.notification.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class HiveTableCreated implements DataHighwayNotification {
  private final String protocolVersion = "1.0";
  private final RoadNotificationType type = RoadNotificationType.HIVE_TABLE_CREATED;
  private final String roadName;
  private final String databaseName;
  private final String tableName;
  private final String partitionColumnName;
  private final String metastoreUris;
  private final String baseLocation;

}
