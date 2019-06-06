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
package com.hotels.road.notification.sns;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import com.hotels.road.notification.model.DataHighwayNotification.RoadNotificationType;
import com.hotels.road.notification.model.HivePartitionCreated;

@RunWith(MockitoJUnitRunner.class)
public class SnsNotificationSenderTest {

  private static final String ROAD_NAME = "ROAD_NAME";
  private static final String PARTITION_SPEC = "PARTITION_SPEC";
  private static final String HIVE_METASTORE_URIS = "HIVE_METASTORE_URIS";
  private static final String DATABASE_NAME = "DATABASE_NAME";
  private static final String LOCATION_URI = "LOCATION_URI";
  private static final String TABLE_NAME = "TABLE_NAME";

  @Mock
  private AmazonSNSAsync sns;
  @Captor
  private ArgumentCaptor<PublishRequest> requestCaptor;

  private SnsNotificationSender snsSender;

  @Test
  public void typical() throws Exception {
    snsSender = new SnsNotificationSender(sns,
        // Topic name
        n -> String.format("topicArn=%s", n.getRoadName()),
        // Subject
        (t, n) -> String.format("subject=%s", n.getRoadName()));

    HivePartitionCreated notification = HivePartitionCreated
        .builder()
        .databaseName(DATABASE_NAME)
        .locationUri(LOCATION_URI)
        .metastoreUris(HIVE_METASTORE_URIS)
        .partitionSpec(PARTITION_SPEC)
        .roadName(ROAD_NAME)
        .tableName(TABLE_NAME)
        .recordCount(1L)
        .build();

    when(sns.publish(requestCaptor.capture())).thenReturn(null);

    snsSender.send(notification);

    PublishRequest request = requestCaptor.getValue();
    assertThat(request.getTopicArn(), is("topicArn=" + ROAD_NAME));
    assertThat(request.getSubject(), is("subject=" + ROAD_NAME));
    @SuppressWarnings("unchecked")
    Map<String, Object> messageMap = new ObjectMapper().readValue(request.getMessage(), HashMap.class);
    assertThat(messageMap.keySet(), is(ImmutableSet.of("protocolVersion", "type", "roadName", "metastoreUris",
        "databaseName", "tableName", "partitionSpec", "locationUri", "recordCount")));
    assertThat(messageMap, hasEntry("protocolVersion", "1.0"));
    assertThat(messageMap, hasEntry("type", RoadNotificationType.HIVE_PARTITION_CREATED.name()));
    assertThat(messageMap, hasEntry("roadName", ROAD_NAME));
    assertThat(messageMap, hasEntry("metastoreUris", HIVE_METASTORE_URIS));
    assertThat(messageMap, hasEntry("databaseName", DATABASE_NAME));
    assertThat(messageMap, hasEntry("tableName", TABLE_NAME));
    assertThat(messageMap, hasEntry("partitionSpec", PARTITION_SPEC));
    assertThat(messageMap, hasEntry("locationUri", LOCATION_URI));
    assertThat(messageMap, hasEntry("recordCount", 1));

    Map<String, MessageAttributeValue> messageAttributes = request.getMessageAttributes();
    assertThat(messageAttributes.get(SnsNotificationSender.PROTOCOL_VERSION).getStringValue(), is("1.0"));
    assertThat(messageAttributes.get(SnsNotificationSender.PROTOCOL_VERSION).getDataType(), is("String"));
    assertThat(messageAttributes.get(SnsNotificationSender.TYPE).getStringValue(), is("HIVE_PARTITION_CREATED"));
    assertThat(messageAttributes.get(SnsNotificationSender.TYPE).getDataType(), is("String"));
    assertThat(messageAttributes.get(SnsNotificationSender.ROAD_NAME).getStringValue(), is("ROAD_NAME"));
    assertThat(messageAttributes.get(SnsNotificationSender.ROAD_NAME).getDataType(), is("String"));
  }

}
