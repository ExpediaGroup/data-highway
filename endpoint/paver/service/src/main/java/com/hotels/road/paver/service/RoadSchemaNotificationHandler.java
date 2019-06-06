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
package com.hotels.road.paver.service;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.hotels.road.model.core.Road;
import com.hotels.road.notification.NotificationHandler;
import com.hotels.road.notification.model.RoadCreatedNotification;
import com.hotels.road.notification.model.SchemaVersionAdded;
import com.hotels.road.notification.model.SchemaVersionDeleted;

@Component
@RequiredArgsConstructor
public class RoadSchemaNotificationHandler {

  private final NotificationHandler notificationHandler;

  public void handleRoadCreated(Road road) {
    notificationHandler.send(RoadCreatedNotification
        .builder()
        .roadName(road.getName())
        .contactEmail(road.getContactEmail())
        .description(road.getDescription())
        .teamName(road.getTeamName())
        .build());
  }

  public void handleSchemaCreated(String roadName, int schemaVersion) {
    SchemaVersionAdded schemaVersionAdded = SchemaVersionAdded
        .builder()
        .roadName(roadName)
        .schemaVersion(schemaVersion)
        .build();
    notificationHandler.send(schemaVersionAdded);
  }

  public void handleSchemaDeleted(String roadName, int schemaVersion) {
    SchemaVersionDeleted schemaNotification = SchemaVersionDeleted
        .builder()
        .roadName(roadName)
        .schemaVersion(schemaVersion)
        .build();
    notificationHandler.send(schemaNotification);
  }

}
