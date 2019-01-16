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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.ReplicaInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction.Replica;

@RunWith(MockitoJUnitRunner.class)
public class ReplicaByPartitionFunctionTest {
  private @Mock AdminClient adminClient;
  private @Mock DescribeLogDirsResult describeLogDirsResult;

  private final int brokerId = 0;
  private final Collection<Integer> brokerIds = singleton(brokerId);

  private ReplicaByPartitionFunction underTest;

  @Before
  public void before() throws Exception {
    underTest = new ReplicaByPartitionFunction(adminClient);
  }

  @Test
  public void typical() throws Exception {
    ReplicaInfo ri = new ReplicaInfo(42L, 0L, false);
    TopicPartition topicPartition = new TopicPartition("topic", 0);
    Map<TopicPartition, ReplicaInfo> replicaInfos = Collections.singletonMap(topicPartition, ri);
    LogDirInfo ldi = new LogDirInfo(null, replicaInfos);
    Map<String, LogDirInfo> ldis = Collections.singletonMap("logDir", ldi);
    KafkaFuture<Map<String, LogDirInfo>> kafkaFuture = KafkaFuture.completedFuture(ldis);
    Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> values = Collections.singletonMap(brokerId, kafkaFuture);

    doReturn(describeLogDirsResult).when(adminClient).describeLogDirs(brokerIds);
    doReturn(values).when(describeLogDirsResult).values();

    Map<TopicPartition, Replica> result = underTest.apply(brokerId);

    assertThat(result.size(), is(1));
    Replica replica = new Replica("logDir", 42L);
    assertThat(result.get(topicPartition), is(replica));
  }

}
