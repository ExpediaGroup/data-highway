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

import static java.util.Collections.singletonMap;

import java.time.Clock;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.hotels.road.agents.trafficcop.TrafficCopConfiguration;
import com.hotels.road.boot.DataHighwayApplication;
import com.hotels.road.trafficcontrol.function.MessageCountPerTopicFunction;

import kafka.utils.ZkUtils;

@SpringBootApplication
@EnableScheduling
@Import(TrafficCopConfiguration.class)
public class TrafficControlApp {
  @Bean(destroyMethod = "close")
  public ZkUtils zkUtils(
      @Value("${kafka.zookeeper}") String zkUrl,
      @Value("${kafka.sessionTimeout:60000}") int sessionTimeout,
      @Value("${kafka.connectionTimeout:60000}") int connectionTimeout,
      @Value("${kafka.zkSecurityEnabled:false}") boolean zkSecurityEnabled) {
    return ZkUtils.apply(zkUrl, sessionTimeout, connectionTimeout, zkSecurityEnabled);
  }

  @Bean
  public KafkaAdminClient kafkaAdminClient(
      ZkUtils zkUtils,
      @Value("${kafka.default.partitions:6}") int defaultPartitions,
      @Value("${kafka.default.replicationFactor:3}") int defaultFeplicationFactor,
      @Value("${kafka.default.topicConfig:#{null}}") Properties defaultTopicConfig,
      MessageCountPerTopicFunction messageCountPerTopicFunction,
      @Value("#{clock}")Clock clock) {
    defaultTopicConfig = Optional.ofNullable(defaultTopicConfig).orElse(new Properties());
    return new KafkaAdminClient(zkUtils, defaultPartitions, defaultFeplicationFactor, defaultTopicConfig,
      messageCountPerTopicFunction, clock);
  }

  @Bean
  public KafkaConsumer<?, ?> kafkaConsumer(@Value("${kafka.bootstrapServers}") String bootstrapServers) {
    Deserializer<byte[]> deserializer = new ByteArrayDeserializer();
    return new KafkaConsumer<>(singletonMap("bootstrap.servers", bootstrapServers), deserializer, deserializer);
  }

  public static void main(String[] args) {
    DataHighwayApplication.run(TrafficControlApp.class, args);
  }
}
