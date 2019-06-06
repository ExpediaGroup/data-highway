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
package com.hotels.road.notification;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.road.notification.model.RoadCreatedNotification;

@RunWith(MockitoJUnitRunner.class)
public class CompositeNotificationHandlerTest {

  private NotificationHandler underTest;
  @Mock
  private NotificationSender notificationSender;

  @Mock
  private NotificationSender notificationSender2;

  @Test
  public void typical() throws Exception {
    underTest = new NotificationHandler(ImmutableList.of(notificationSender));
    RoadCreatedNotification roadNotification = RoadCreatedNotification.builder().build();
    underTest.send(roadNotification);
    verify(notificationSender).send(roadNotification);
  }

  @Test
  public void typicalComposite() throws Exception {
    underTest = new NotificationHandler(ImmutableList.of(notificationSender, notificationSender2));
    RoadCreatedNotification roadNotification = RoadCreatedNotification.builder().build();
    underTest.send(roadNotification);
    verify(notificationSender).send(roadNotification);
    verify(notificationSender2).send(roadNotification);
  }
}
