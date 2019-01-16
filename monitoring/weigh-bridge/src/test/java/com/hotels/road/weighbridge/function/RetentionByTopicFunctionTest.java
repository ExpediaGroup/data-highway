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
package com.hotels.road.weighbridge.function;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RetentionByTopicFunctionTest {
  private @Mock AdminClient adminClient;
  private @Mock DescribeConfigsResult describeConfigsResult;

  private RetentionByTopicFunction underTest;

  @Before
  public void before() throws Exception {
    underTest = new RetentionByTopicFunction(adminClient);
  }

  @Test
  public void typical() throws Exception {
    String topic = "topic";
    Collection<String> topics = singleton(topic);
    ConfigResource configResource = new ConfigResource(TOPIC, topic);
    Config config = new Config(singleton(new ConfigEntry("retention.ms", "1")));
    KafkaFuture<Map<ConfigResource, Config>> kafkaFuture = completedFuture(singletonMap(configResource, config));

    doReturn(describeConfigsResult).when(adminClient).describeConfigs(any());
    doReturn(kafkaFuture).when(describeConfigsResult).all();

    Map<String, Duration> result = underTest.apply(topics);

    assertThat(result.size(), is(1));
    Duration retention = result.get(topic);
    assertThat(retention, is(Duration.ofMillis(1)));
  }

}
