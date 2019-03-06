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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.rest.model.RoadType;
import com.hotels.road.trafficcontrol.model.KafkaRoad;
import com.hotels.road.trafficcontrol.model.MessageStatus;
import com.hotels.road.trafficcontrol.model.TrafficControlStatus;

public class KafkaModelReaderTest {
  ObjectMapper mapper = new ObjectMapper();
  KafkaModelReader reader = new KafkaModelReader();

  @Test
  public void correct_decode_of_full_model() throws Exception {
    JsonNode json = mapper
        .readTree(
            "{\n"
                + "  \"name\": \"test_topic6\",\n"
                + "  \"topicName\": \"road.test_topic6\",\n"
                + "  \"description\": \"test road\",\n"
                + "  \"type\":\"COMPACT\",\n"
                + "  \"status\": {\n"
                + "    \"topicCreated\": true,\n"
                + "    \"partitions\": 6,\n"
                + "    \"replicationFactor\": 3,\n"
                + "    \"message\": \"\"\n"
                + "  },\n"
                + "  \"messageStatus\": {\n"
                + "    \"lastUpdated\": 124947,\n"
                + "    \"numberOfMessages\": 30\n"
                + "  },\n"
                + "  \"deleted\": false\n"
                + "}");
    KafkaRoad road = reader.read(json);

    assertThat(
        road,
        is(
            new KafkaRoad(
                "test_topic6",
                "road.test_topic6",
                RoadType.COMPACT,
                new TrafficControlStatus(true, 6, 3, ""),
                new MessageStatus(124947, 30), false)));
  }

  @Test
  public void correct_decode_with_null_status() throws Exception {
    JsonNode json = mapper
        .readTree(
            "{\n"
                + "  \"name\": \"test_topic6\",\n"
                + "  \"topicName\": \"road.test_topic6\",\n"
                + "  \"type\":\"COMPACT\",\n"
                + "  \"status\": null,\n"
                + "  \"messageStatus\": null,\n"
                + "  \"deleted\": true\n"
                + "}");
    KafkaRoad road = reader.read(json);

    assertThat(road, is(new KafkaRoad("test_topic6", "road.test_topic6", RoadType.COMPACT, null, null, true)));
  }
}
