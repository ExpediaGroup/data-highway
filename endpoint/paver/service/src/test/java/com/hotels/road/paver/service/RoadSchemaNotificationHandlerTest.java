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
package com.hotels.road.paver.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.model.core.Road;
import com.hotels.road.notification.NotificationHandler;
import com.hotels.road.notification.model.DataHighwayNotification;
import com.hotels.road.notification.model.DataHighwayNotification.RoadNotificationType;
import com.hotels.road.notification.model.RoadCreatedNotification;
import com.hotels.road.notification.model.SchemaVersionAdded;
import com.hotels.road.notification.model.SchemaVersionDeleted;

@RunWith(MockitoJUnitRunner.class)
public class RoadSchemaNotificationHandlerTest {

  private static final String ROAD_NAME = "test_road";

  private static final String EMAIL = "team@hotels.com";

  private static final String DESCRIPTION = "A test road";

  private static final String TEAM_NAME = "super team";

  private static final int SCHEMA_VERSION = 5;

  private RoadSchemaNotificationHandler underTest;

  @Mock
  private NotificationHandler notificationHandler;

  @Before
  public void setUp() throws Exception {
    underTest = new RoadSchemaNotificationHandler(notificationHandler);
  }

  @Test
  public void roadCreated() throws Exception {
    Road road = new Road();
    road.setName(ROAD_NAME);
    road.setContactEmail(EMAIL);
    road.setDescription(DESCRIPTION);
    road.setTeamName(TEAM_NAME);
    underTest.handleRoadCreated(road);
    ArgumentCaptor<DataHighwayNotification> captor = ArgumentCaptor.forClass(DataHighwayNotification.class);
    verify(notificationHandler).send(captor.capture());
    RoadCreatedNotification notification = (RoadCreatedNotification) captor.getValue();
    assertThat(notification.getType(), is(RoadNotificationType.ROAD_CREATED));
    assertThat(notification.getRoadName(), is(ROAD_NAME));
    assertThat(notification.getContactEmail(), is(EMAIL));
    assertThat(notification.getDescription(), is(DESCRIPTION));
    assertThat(notification.getTeamName(), is(TEAM_NAME));
  }

  @Test
  public void schemaCreated() throws Exception {
    underTest.handleSchemaCreated(ROAD_NAME, SCHEMA_VERSION);
    ArgumentCaptor<DataHighwayNotification> captor = ArgumentCaptor.forClass(DataHighwayNotification.class);
    verify(notificationHandler).send(captor.capture());
    SchemaVersionAdded notification = (SchemaVersionAdded) captor.getValue();
    assertThat(notification.getSchemaVersion(), is(SCHEMA_VERSION));
    assertThat(notification.getType(), is(RoadNotificationType.SCHEMA_VERSION_ADDED));
  }

  @Test
  public void schemaDeleted() throws Exception {
    underTest.handleSchemaDeleted(ROAD_NAME, SCHEMA_VERSION);
    ArgumentCaptor<DataHighwayNotification> captor = ArgumentCaptor.forClass(DataHighwayNotification.class);
    verify(notificationHandler).send(captor.capture());
    SchemaVersionDeleted notification = (SchemaVersionDeleted) captor.getValue();
    assertThat(notification.getSchemaVersion(), is(SCHEMA_VERSION));
    assertThat(notification.getType(), is(RoadNotificationType.SCHEMA_VERSION_DELETED));
  }

}
