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
package com.hotels.road.trafficcontrol;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.rest.model.RoadType;
import com.hotels.road.tollbooth.client.api.Operation;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.trafficcontrol.model.KafkaRoad;
import com.hotels.road.trafficcontrol.model.TrafficControlStatus;

@RunWith(MockitoJUnitRunner.class)
public class TrafficControlTest {
  @Mock
  private KafkaAdminClient kafkaAdminClient;

  private TrafficControl trafficControl;

  static KafkaRoad testRoadModel = new KafkaRoad("test_road", "road.test_road", RoadType.NORMAL, null, null, false);

  @Before
  public void setUp() {
    when(kafkaAdminClient.getPartitions()).thenReturn(6);
    when(kafkaAdminClient.getReplicationFactor()).thenReturn(1);
    trafficControl = new TrafficControl(kafkaAdminClient, "road.");
  }

  @Test
  public void create_road() throws Exception {
    List<PatchOperation> operations = trafficControl.newModel("test_road", testRoadModel);

    verify(kafkaAdminClient).createTopic(testRoadModel);

    assertThat(operations.size(), is(1));
    assertThat(operations.get(0).getOperation(), is(Operation.ADD));
    assertThat(operations.get(0).getPath(), is("/status"));
    assertThat(operations.get(0).getValue(), is(new TrafficControlStatus(true, 6, 1, "")));
  }

  @Test
  public void create_road_generate_topic_name() throws Exception {
    KafkaRoad testRoadModel = new KafkaRoad("test_road", null, RoadType.NORMAL, null, null, false);
    List<PatchOperation> operations = trafficControl.newModel("test_road", testRoadModel);

    verify(kafkaAdminClient).createTopic(TrafficControlTest.testRoadModel);

    assertThat(operations.size(), is(2));
    assertThat(operations.get(0).getOperation(), is(Operation.ADD));
    assertThat(operations.get(0).getPath(), is("/topicName"));
    assertThat(operations.get(0).getValue(), is("road.test_road"));
    assertThat(operations.get(1).getOperation(), is(Operation.ADD));
    assertThat(operations.get(1).getPath(), is("/status"));
    assertThat(operations.get(1).getValue(), is(new TrafficControlStatus(true, 6, 1, "")));
  }

  @Test
  public void create_road_when_topic_exists_works_without_error() throws Exception {
    willThrow(TopicExistsException.class).given(kafkaAdminClient).createTopic(testRoadModel);

    List<PatchOperation> operations = trafficControl.newModel("test_road", testRoadModel);

    verify(kafkaAdminClient).createTopic(testRoadModel);

    assertThat(operations.size(), is(1));
    assertThat(operations.get(0).getOperation(), is(Operation.ADD));
    assertThat(operations.get(0).getPath(), is("/status"));
    assertThat(operations.get(0).getValue(), is(new TrafficControlStatus(true, 6, 1, "")));
  }

  @Test
  public void create_road_fails_when_there_is_a_kafka_error() throws Exception {
    willThrow(KafkaException.class).given(kafkaAdminClient).createTopic(testRoadModel);

    List<PatchOperation> operations = trafficControl.newModel("test_road", testRoadModel);

    verify(kafkaAdminClient).createTopic(testRoadModel);

    assertThat(operations.size(), is(1));
    assertThat(operations.get(0).getOperation(), is(Operation.ADD));
    assertThat(operations.get(0).getPath(), is("/status"));
    TrafficControlStatus status = (TrafficControlStatus) operations.get(0).getValue();
    assertFalse(status.isTopicCreated());
    assertThat(status.getMessage(), containsString(KafkaException.class.getSimpleName()));
  }

  @Test
  public void inspectRoad_creates_topic_if_its_missing() throws Exception {
    given(kafkaAdminClient.topicExists("road.test_road")).willReturn(false);

    List<PatchOperation> operations = trafficControl.inspectModel("test_road", testRoadModel);

    then(kafkaAdminClient).should().createTopic(testRoadModel);

    assertThat(operations.size(), is(1));
    assertThat(operations.get(0).getOperation(), is(Operation.ADD));
    assertThat(operations.get(0).getPath(), is("/status"));
    assertThat(operations.get(0).getValue(), is(new TrafficControlStatus(true, 6, 1, "")));
  }

  @Test
  public void inspectRoad_does_nothing_when_topic_exists_and_is_correct() throws Exception {
    KafkaRoad model = new KafkaRoad("test_road", "road.test_road", RoadType.NORMAL,
        new TrafficControlStatus(true, 12, 3, ""), null, false);

    given(kafkaAdminClient.topicExists("road.test_road")).willReturn(true);
    given(kafkaAdminClient.topicDetails("road.test_road")).willReturn(new KafkaTopicDetails(RoadType.NORMAL, 12, 3));

    List<PatchOperation> operations = trafficControl.inspectModel("test_road", model);

    assertTrue(operations.isEmpty());
  }

  @Test
  public void inspectRoad_updates_status_when_it_does_not_match_topic() throws Exception {
    KafkaRoad model = new KafkaRoad("test_road", "road.test_road", RoadType.NORMAL,
        new TrafficControlStatus(true, 6, 3, ""), null, false);

    given(kafkaAdminClient.topicExists("road.test_road")).willReturn(true);
    given(kafkaAdminClient.topicDetails("road.test_road")).willReturn(new KafkaTopicDetails(RoadType.NORMAL, 12, 3));

    List<PatchOperation> operations = trafficControl.inspectModel("test_road", model);

    assertThat(operations.size(), is(1));
    assertThat(operations.get(0).getOperation(), is(Operation.ADD));
    assertThat(operations.get(0).getPath(), is("/status"));
    assertThat(operations.get(0).getValue(), is(new TrafficControlStatus(true, 12, 3, "")));
  }

  @Test
  public void inspectRoad_corrects_road_type() throws Exception {
    KafkaRoad model = new KafkaRoad("test_road", "road.test_road", RoadType.NORMAL,
        new TrafficControlStatus(true, 6, 3, ""), null, false);

    given(kafkaAdminClient.topicExists("road.test_road")).willReturn(true);
    given(kafkaAdminClient.topicDetails("road.test_road")).willReturn(new KafkaTopicDetails(RoadType.COMPACT, 6, 3));

    List<PatchOperation> operations = trafficControl.inspectModel("test_road", model);

    assertThat(operations.size(), is(1));
    assertThat(operations.get(0).getOperation(), is(Operation.ADD));
    assertThat(operations.get(0).getPath(), is("/type"));
    assertThat(operations.get(0).getValue(), is(RoadType.COMPACT));
  }
}
