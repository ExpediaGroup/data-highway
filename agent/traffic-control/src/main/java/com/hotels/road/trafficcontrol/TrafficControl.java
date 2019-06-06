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
package com.hotels.road.trafficcontrol;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static com.hotels.road.tollbooth.client.api.PatchOperation.add;
import static com.hotels.road.tollbooth.client.api.PatchOperation.remove;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.rest.model.RoadType;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.trafficcontrol.model.KafkaRoad;
import com.hotels.road.trafficcontrol.model.TrafficControlStatus;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TrafficControl implements Agent<KafkaRoad> {
  private static final String CREATE_EXCEPTION_FORMAT = "Error creating Kafka topic \"%s\"";
  private static final String UPDATE_EXCEPTION_FORMAT = "Error updating Kafka topic \"%s\"";
  private static final String TOPIC_NAME_PATH = "/topicName";
  private static final String KAFKA_STATUS_PATH = "/status";
  private static final String ROAD_TYPE_PATH = "/type";

  private final KafkaAdminClient adminClient;
  private final String topicPrefix;

  @Autowired
  public TrafficControl(KafkaAdminClient kafkaAdminClient, @Value("${kafka.topic.prefix:road.}") String topicPrefix) {
    adminClient = kafkaAdminClient;
    this.topicPrefix = topicPrefix;
    log.info("{} started.", this.getClass());
  }

  @Override
  public List<PatchOperation> newModel(String key, KafkaRoad newModel) {
    return handleModel(newModel);
  }

  @Override
  public List<PatchOperation> updatedModel(String key, KafkaRoad oldModel, KafkaRoad newModel) {
    return handleModel(newModel);
  }

  @Override
  public void deletedModel(String key, KafkaRoad oldModel) {
    if(!oldModel.isDeleted()) {
      log.warn("Model for Road {} was deleted but had its deleted flag disabled", oldModel.getName());
    }
  }

  @Override
  public List<PatchOperation> inspectModel(String key, KafkaRoad model) {
    return handleModel(model);
  }

  private List<PatchOperation> handleModel(KafkaRoad road) {
    List<PatchOperation> result = new ArrayList<>();

    if(isRoadDeleted(road)) {
      result.add(remove(""));
      return result;
    }

    String topicName = road.getTopicName();
    if (topicName == null) {
      topicName = topicPrefix + road.getName();
      result.add(add(TOPIC_NAME_PATH, topicName));
      road = road.withTopicName(topicName);
    }

    TrafficControlStatus status;
    if (adminClient.topicExists(topicName)) {
      result.addAll(checkModelType(road));
      status = checkAndUpdateTopic(road);
    } else {
      status = createTopic(road);
    }

    if (!status.equals(road.getStatus())) {
      result.add(add(KAFKA_STATUS_PATH, status));
    }

    return result;
  }

  private boolean isRoadDeleted(KafkaRoad road) {
    if(road.isDeleted()) {
      String topicName = road.getTopicName();
      if (adminClient.topicExists(topicName)) {
        try {
          adminClient.deleteTopic(topicName);
        } catch (Exception e) {
          log.warn("Road deletion failed for topic {}", topicName);
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private List<PatchOperation> checkModelType(KafkaRoad road) {
    RoadType roadType = adminClient.topicDetails(road.getTopicName()).getType();
    if (road.getType() != roadType) {
      if (road.getType() != null) {
        log.error("Road type for road {} was changed. Changing road type is not supported!", road.getName());
      }
      return singletonList(add(ROAD_TYPE_PATH, roadType));
    }
    return emptyList();
  }

  private TrafficControlStatus checkAndUpdateTopic(KafkaRoad road) {
    String message = "";
    try {
      adminClient.checkAndUpdateTopic(road);
    } catch (KafkaException e) {
      log.warn("Problem updating Kafka topic for {}", road.getName(), e);
      message = String.format(UPDATE_EXCEPTION_FORMAT, e.toString());
    }
    KafkaTopicDetails topicDetails = adminClient.topicDetails(road.getTopicName());
    return status(true, topicDetails.getNumPartitions(), topicDetails.getNumReplicas(), message);
  }

  private TrafficControlStatus createTopic(KafkaRoad road) {
    try {
      try {
        adminClient.createTopic(road);
      } catch (TopicExistsException e) {
        // Ignore, topic is already created, send notifications again.
      }
      return status(true, adminClient.getPartitions(), adminClient.getReplicationFactor(), "");
    } catch (KafkaException e) {
      log.warn("Problem creating Kafka topic for {}", road.getName(), e);
      return status(false, 0, 0, String.format(CREATE_EXCEPTION_FORMAT, e.toString()));
    }
  }

  private TrafficControlStatus status(boolean created, int partitions, int replicas, String message) {
    return new TrafficControlStatus(created, partitions, replicas, message);
  }
}
