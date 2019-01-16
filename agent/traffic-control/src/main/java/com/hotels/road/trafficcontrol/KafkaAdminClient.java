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

import static java.util.Optional.ofNullable;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.TopicConfig;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.KafkaException;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.hotels.road.rest.model.RoadType;
import com.hotels.road.trafficcontrol.model.KafkaRoad;

@Slf4j
@RequiredArgsConstructor
public class KafkaAdminClient {
  private static final String LEADER_THROTTLED_REPLICAS = LogConfig.LeaderReplicationThrottledReplicasProp();
  private static final String FOLLOWER_THROTTLED_REPLICAS = LogConfig.FollowerReplicationThrottledReplicasProp();
  private static final String WILDCARD = "*";

  /*
   * AdminUtils.createTopic$default$6() is the method that is generated for the default RackAwareMode.Enforced mode
   * parameter for the Scala AdminUtils.createTopic method.
   */
  private static final RackAwareMode DEFAULT_RACK_AWARE_MODE = AdminUtils.createTopic$default$6();

  private final ZkUtils zkUtils;
  private final int partitions;
  private final int replicationFactor;
  private final Properties defaultTopicConfig;

  public void createTopic(KafkaRoad road) throws KafkaException {
    Properties topicConfig = new Properties(defaultTopicConfig);
    topicConfig.setProperty(LEADER_THROTTLED_REPLICAS, WILDCARD);
    topicConfig.setProperty(FOLLOWER_THROTTLED_REPLICAS, WILDCARD);
    RoadType roadType = ofNullable(road.getType()).orElse(RoadType.NORMAL);
    switch (roadType) {
    case NORMAL:
      topicConfig.setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
      break;
    case COMPACT:
      topicConfig.setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
      break;
    default:
      throw new KafkaException("Unhandled road type \"" + road.getType().name() + "\"");
    }
    AdminUtils
        .createTopic(zkUtils, road.getTopicName(), partitions, replicationFactor, topicConfig, DEFAULT_RACK_AWARE_MODE);
    log.info("Created {} topic {}", roadType, road.getTopicName());
  }

  public void checkAndUpdateTopic(KafkaRoad road) throws KafkaException {
    Properties topicConfig = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), road.getTopicName());
    boolean modified = false;
    modified |= checkAndUpdateThrottledReplicas(topicConfig, LEADER_THROTTLED_REPLICAS);
    modified |= checkAndUpdateThrottledReplicas(topicConfig, FOLLOWER_THROTTLED_REPLICAS);
    if (modified) {
      AdminUtils.changeTopicConfig(zkUtils, road.getTopicName(), topicConfig);
      log.info("Updated topic {}", road.getTopicName());
    }
  }

  private boolean checkAndUpdateThrottledReplicas(Properties topicConfig, String property) {
    Object oldValue = topicConfig.setProperty(property, WILDCARD);
    return !WILDCARD.equals(oldValue);
  }

  public boolean topicExists(String name) {
    return AdminUtils.topicExists(zkUtils, name);
  }

  public KafkaTopicDetails topicDetails(String topic) {
    scala.collection.Map<String, scala.collection.Map<Object, scala.collection.Seq<Object>>> partitionAssignmentForTopics = zkUtils
        .getPartitionAssignmentForTopics(scala.collection.immutable.Nil$.MODULE$.$colon$colon(topic));
    scala.collection.Map<Object, scala.collection.Seq<Object>> topicPartitionAssignment = partitionAssignmentForTopics
        .iterator()
        .next()._2;
    Map<Object, scala.collection.Seq<Object>> map = scala.collection.JavaConversions
        .mapAsJavaMap(topicPartitionAssignment);
    int numPartitions = map.size();
    int numReplicas = map.get(0).size();

    RoadType type;
    Properties topicConfig = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
    String cleanupPolicyConfig = topicConfig
        .getProperty(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    switch (cleanupPolicyConfig) {
    case TopicConfig.CLEANUP_POLICY_COMPACT:
      type = RoadType.COMPACT;
      break;
    default:
      type = RoadType.NORMAL;
    }
    log.debug("numPartitions: {}, numReplicas: {}", numPartitions, numReplicas);
    return new KafkaTopicDetails(type, numPartitions, numReplicas);
  }

  public int getPartitions() {
    return partitions;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }
}
