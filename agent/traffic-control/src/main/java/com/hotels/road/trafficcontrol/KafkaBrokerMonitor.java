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

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import kafka.admin.AdminUtils;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.server.ConfigType;
import kafka.server.DynamicConfig;
import kafka.utils.ZkUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConversions;
import scala.collection.Seq;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaBrokerMonitor {
  private static final String LEADER_THROTTLED_RATE = DynamicConfig.Broker$.MODULE$
      .LeaderReplicationThrottledRateProp();
  private static final String FOLLOWER_THROTTLED_RATE = DynamicConfig.Broker$.MODULE$
      .FollowerReplicationThrottledRateProp();
  private static final String BROKER_TYPE = ConfigType.Broker();

  private final AdminClient adminClient;
  private final String leaderThrottledRateBytes;
  private final String followerThrottledRateBytes;

  @Autowired
  public KafkaBrokerMonitor(
      ZkUtils zkUtils,
      // 52428800 = 50MiB
      @Value("${kafka.throttle.leader.rate:52428800}") String leaderThrottledRateBytes,
      @Value("${kafka.throttle.follower.rate:52428800}") String followerThrottledRateBytes) {
    this(new AdminClient(zkUtils), leaderThrottledRateBytes, followerThrottledRateBytes);
  }

  @Scheduled(fixedDelayString = "${kafka.broker.monitor.fixed.delay:60000}")
  public void checkAndUpdateBrokers() throws KafkaException {
    adminClient.getBrokerIds().forEach(this::checkAndUpdateBroker);
  }

  private void checkAndUpdateBroker(String brokerId) {
    Properties config = adminClient.getConfig(brokerId);
    boolean modified = false;
    if (checkAndUpdateThrottledRate(brokerId, config, LEADER_THROTTLED_RATE, leaderThrottledRateBytes)) {
      modified = true;
    }
    if (checkAndUpdateThrottledRate(brokerId, config, FOLLOWER_THROTTLED_RATE, followerThrottledRateBytes)) {
      modified = true;
    }
    if (modified) {
      adminClient.changeConfig(brokerId, config);
    }
  }

  private boolean checkAndUpdateThrottledRate(String brokerId, Properties brokerConfig, String property, String rate) {
    String value = brokerConfig.getProperty(property);
    if (!rate.equals(value)) {
      log.info("Updating property '{}' from '{}' to '{}' on broker {}.", property, value, rate, brokerId);
      brokerConfig.setProperty(property, rate);
      return true;
    }
    return false;
  }

  @RequiredArgsConstructor
  static class AdminClient {
    private final ZkUtils zkUtils;

    List<String> getBrokerIds() {
      return JavaConversions
          .seqAsJavaList(zkUtils.getAllBrokersInCluster())
          .stream()
          .map(Broker::id)
          .map(Object::toString)
          .collect(toList());
    }

    Properties getConfig(String brokerId) {
      return AdminUtils.fetchEntityConfig(zkUtils, BROKER_TYPE, brokerId);
    }

    void changeConfig(String brokerId, Properties config) {
      Seq<Object> brokers = JavaConversions.collectionAsScalaIterable(singletonList((Object) brokerId)).toSeq();
      AdminUtils.changeBrokerConfig(zkUtils, brokers, config);
    }
  }
}
