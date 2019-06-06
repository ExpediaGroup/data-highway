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
package com.hotels.road.weighbridge.function;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.weighbridge.function.LeaderInSyncByPartitionFunction.LeaderInSync;

@RunWith(MockitoJUnitRunner.class)
public class LeaderInSyncByPartitionFunctionTest {
  private @Mock AdminClient adminClient;
  private @Mock DescribeTopicsResult describeTopicsResult;

  private final String topic = "topic1";
  private final Collection<String> topics = singleton(topic);
  private final Node node0 = new Node(0, "host0", 9092);
  private final Node node1 = new Node(1, "host1", 9092);

  private LeaderInSyncByPartitionFunction underTest;

  @Before
  public void before() throws Exception {
    underTest = new LeaderInSyncByPartitionFunction(adminClient);
  }

  @Test
  public void typical() throws Exception {
    TopicPartitionInfo tpi = new TopicPartitionInfo(0, node0, singletonList(node0), singletonList(node0));
    KafkaFuture<Map<String, TopicDescription>> kafkaFuture = topicDescriptionFuture(tpi);

    doReturn(describeTopicsResult).when(adminClient).describeTopics(topics);
    doReturn(kafkaFuture).when(describeTopicsResult).all();

    Map<TopicPartition, LeaderInSync> result = underTest.apply(0, topics);

    assertThat(result.size(), is(1));
    LeaderInSync leaderInSync = result.get(new TopicPartition(topic, 0));
    assertThat(leaderInSync.isLeader(), is(true));
    assertThat(leaderInSync.isInSync(), is(true));
  }

  @Test
  public void notLocalBroker() throws Exception {
    TopicPartitionInfo tpi = new TopicPartitionInfo(0, node0, singletonList(node1), singletonList(node0));
    KafkaFuture<Map<String, TopicDescription>> kafkaFuture = topicDescriptionFuture(tpi);

    doReturn(describeTopicsResult).when(adminClient).describeTopics(topics);
    doReturn(kafkaFuture).when(describeTopicsResult).all();

    Map<TopicPartition, LeaderInSync> result = underTest.apply(0, topics);

    assertThat(result.size(), is(0));
  }

  @Test
  public void notLeader() throws Exception {
    TopicPartitionInfo tpi = new TopicPartitionInfo(0, node1, singletonList(node0), singletonList(node0));
    KafkaFuture<Map<String, TopicDescription>> kafkaFuture = topicDescriptionFuture(tpi);

    doReturn(describeTopicsResult).when(adminClient).describeTopics(topics);
    doReturn(kafkaFuture).when(describeTopicsResult).all();

    Map<TopicPartition, LeaderInSync> result = underTest.apply(0, topics);

    assertThat(result.size(), is(1));
    LeaderInSync leaderInSync = result.get(new TopicPartition(topic, 0));
    assertThat(leaderInSync.isLeader(), is(false));
    assertThat(leaderInSync.isInSync(), is(true));
  }

  @Test
  public void notInSync() throws Exception {
    TopicPartitionInfo tpi = new TopicPartitionInfo(0, node0, singletonList(node0), singletonList(node1));
    KafkaFuture<Map<String, TopicDescription>> kafkaFuture = topicDescriptionFuture(tpi);

    doReturn(describeTopicsResult).when(adminClient).describeTopics(topics);
    doReturn(kafkaFuture).when(describeTopicsResult).all();

    Map<TopicPartition, LeaderInSync> result = underTest.apply(0, topics);

    assertThat(result.size(), is(1));
    LeaderInSync leaderInSync = result.get(new TopicPartition(topic, 0));
    assertThat(leaderInSync.isLeader(), is(true));
    assertThat(leaderInSync.isInSync(), is(false));
  }

  @Test
  public void noLeader() throws Exception {
    TopicPartitionInfo tpi = new TopicPartitionInfo(0, null, singletonList(node0), singletonList(node0));
    KafkaFuture<Map<String, TopicDescription>> kafkaFuture = topicDescriptionFuture(tpi);

    doReturn(describeTopicsResult).when(adminClient).describeTopics(topics);
    doReturn(kafkaFuture).when(describeTopicsResult).all();

    Map<TopicPartition, LeaderInSync> result = underTest.apply(0, topics);

    assertThat(result.size(), is(1));
    LeaderInSync leaderInSync = result.get(new TopicPartition(topic, 0));
    assertThat(leaderInSync.isLeader(), is(false));
    assertThat(leaderInSync.isInSync(), is(true));
  }

  private KafkaFuture<Map<String, TopicDescription>> topicDescriptionFuture(TopicPartitionInfo tpi) {
    List<TopicPartitionInfo> partitions = singletonList(tpi);
    TopicDescription td = new TopicDescription(topic, false, partitions);
    KafkaFuture<Map<String, TopicDescription>> kafkaFuture = completedFuture(singletonMap(topic, td));
    return kafkaFuture;
  }
}
