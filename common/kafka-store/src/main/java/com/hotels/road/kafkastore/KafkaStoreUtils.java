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
package com.hotels.road.kafkastore;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

public final class KafkaStoreUtils {
  static final int SESSION_TIMEOUT_MS = (int) SECONDS.toMillis(10);
  static final int CONNECTION_TIMEOUT_MS = (int) SECONDS.toMillis(8);
  static final boolean IS_SECURE_KAFKA_CLUSTER = false;

  private KafkaStoreUtils() {}

  public static void checkAndCreateTopic(String zkConnect, String topic, int replicas) {
    ZkClient zkClient = new ZkClient(zkConnect, SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS, ZKStringSerializer$.MODULE$);
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), IS_SECURE_KAFKA_CLUSTER);

    if (AdminUtils.topicExists(zkUtils, topic)) {
      verifyTopic(zkUtils, topic);
      return;
    }

    int partitions = 1;
    Properties topicConfig = new Properties();
    topicConfig.put(LogConfig.CleanupPolicyProp(), "compact");

    AdminUtils.createTopic(zkUtils, topic, partitions, replicas, topicConfig, RackAwareMode.Enforced$.MODULE$);

    zkClient.close();
    zkUtils.close();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void verifyTopic(ZkUtils zkUtils, String topic) {
    Set topics = new HashSet();
    topics.add(topic);

    // check # partition and the replication factor
    scala.collection.mutable.Map partitionAssignmentForTopics = zkUtils
        .getPartitionAssignmentForTopics(JavaConversions.asScalaSet(topics).toSeq());
    scala.collection.Map partitionAssignment = (scala.collection.Map) partitionAssignmentForTopics.get(topic).get();

    if (partitionAssignment.size() != 1) {
      throw new RuntimeException(String.format("The schema topic %s should have only 1 partition.", topic));
    }

    // check the retention policy
    Properties prop = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
    String retentionPolicy = prop.getProperty(LogConfig.CleanupPolicyProp());
    if (retentionPolicy == null || "compact".compareTo(retentionPolicy) != 0) {
      throw new RuntimeException(String.format("The retention policy of the schema topic %s must be compact.", topic));
    }
  }
}
