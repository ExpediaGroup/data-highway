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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static com.hotels.road.kafkastore.KafkaStoreUtils.CONNECTION_TIMEOUT_MS;
import static com.hotels.road.kafkastore.KafkaStoreUtils.IS_SECURE_KAFKA_CLUSTER;
import static com.hotels.road.kafkastore.KafkaStoreUtils.SESSION_TIMEOUT_MS;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.ClassRule;
import org.junit.Test;

import kafka.admin.AdminUtils;
import kafka.log.LogConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaStoreUtilsTest {

  private static final int REPLICAS = 1;
  private static final int NUM_BROKERS = 1;

  @ClassRule
  public static final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

  @Test
  public void topicIsCreated() {
    String topic = "topic1";
    KafkaStoreUtils.checkAndCreateTopic(cluster.zKConnectString(), topic, REPLICAS);
    assertThat(topicExists(topic), is(true));
  }

  @Test
  public void topicAlreadyExists() throws Exception {
    String topic = "topic2";
    int partitions = 1;
    Properties props = new Properties();
    props.setProperty(LogConfig.CleanupPolicyProp(), "compact");
    cluster.createTopic(topic, partitions, 1, props);
    try {
      KafkaStoreUtils.checkAndCreateTopic(cluster.zKConnectString(), topic, REPLICAS);
    } catch (RuntimeException e) {
      fail();
    }
  }

  @Test(expected = RuntimeException.class)
  public void topicAlreadyExistsButWrongPartitions() throws Exception {
    String topic = "topic3";
    int partitions = 2;
    Properties props = new Properties();
    props.setProperty(LogConfig.CleanupPolicyProp(), "compact");
    cluster.createTopic(topic, partitions, 1, props);
    KafkaStoreUtils.checkAndCreateTopic(cluster.zKConnectString(), topic, REPLICAS);
  }

  @Test(expected = RuntimeException.class)
  public void topicAlreadyExistsButNotCompact() throws Exception {
    String topic = "topic4";
    int partitions = 1;
    cluster.createTopic(topic, partitions, 1);
    KafkaStoreUtils.checkAndCreateTopic(cluster.zKConnectString(), topic, REPLICAS);
  }

  private boolean topicExists(String topic) {
    ZkClient zkClient = new ZkClient(cluster.zKConnectString(), SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS,
        ZKStringSerializer$.MODULE$);
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(cluster.zKConnectString()), IS_SECURE_KAFKA_CLUSTER);
    boolean exists = AdminUtils.topicExists(zkUtils, topic);
    zkClient.close();
    zkUtils.close();
    return exists;
  }

}
