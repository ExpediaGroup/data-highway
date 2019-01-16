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
package com.hotels.road.weighbridge;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.weighbridge.function.BrokerNodeFunction;
import com.hotels.road.weighbridge.function.BrokerNodeFunction.BrokerNode;
import com.hotels.road.weighbridge.function.DiskByLogDirFunction;
import com.hotels.road.weighbridge.function.LeaderInSyncByPartitionFunction;
import com.hotels.road.weighbridge.function.LeaderInSyncByPartitionFunction.LeaderInSync;
import com.hotels.road.weighbridge.function.OffsetsByPartitionFunction;
import com.hotels.road.weighbridge.function.OffsetsByPartitionFunction.Offsets;
import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction;
import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction.Replica;
import com.hotels.road.weighbridge.function.RetentionByTopicFunction;
import com.hotels.road.weighbridge.function.SizeByPartitionFunction;
import com.hotels.road.weighbridge.model.Broker;
import com.hotels.road.weighbridge.model.LogDir;
import com.hotels.road.weighbridge.model.PartitionReplica;
import com.hotels.road.weighbridge.model.Topic;

@RunWith(MockitoJUnitRunner.class)
public class BrokerSupplierTest {
  private final Predicate<String> hostPredicate = host -> true;
  private @Mock BrokerNodeFunction brokerNodeFunction;
  private @Mock ReplicaByPartitionFunction replicaByPartitionFunction;
  private @Mock LeaderInSyncByPartitionFunction leaderInSyncByPartitionFunction;
  private @Mock RetentionByTopicFunction retentionByTopicFunction;
  private @Mock OffsetsByPartitionFunction offsetsByPartitionFunction;
  private @Mock SizeByPartitionFunction sizeByPartitionFunction;
  private @Mock DiskByLogDirFunction diskByLogDirFunction;

  private BrokerSupplier underTest;

  @Test
  public void typical() throws Exception {
    String t = "topic";
    Collection<String> ts = singleton(t);
    TopicPartition tp = new TopicPartition(t, 0);
    Collection<TopicPartition> tps = singleton(tp);
    Map<TopicPartition, Replica> replicaByPartition = singletonMap(tp, new Replica("logDir", 42L));

    doReturn(new BrokerNode(0, "rack", "host")).when(brokerNodeFunction).apply(hostPredicate);
    doReturn(replicaByPartition).when(replicaByPartitionFunction).apply(0);
    doReturn(singletonMap(tp, new LeaderInSync(false, true))).when(leaderInSyncByPartitionFunction).apply(0, ts);
    doReturn(singletonMap(t, Duration.ofMillis(1))).when(retentionByTopicFunction).apply(ts);
    doReturn(singletonMap(tp, new Offsets(1L, 3L))).when(offsetsByPartitionFunction).apply(tps);
    doReturn(singletonMap(tp, 12L)).when(sizeByPartitionFunction).apply(replicaByPartition);
    doReturn(singletonMap("logDir", new DiskByLogDirFunction.Disk(123L, 234L))).when(diskByLogDirFunction).apply(
        replicaByPartition);

    underTest = new BrokerSupplier(hostPredicate, brokerNodeFunction, replicaByPartitionFunction,
        leaderInSyncByPartitionFunction, retentionByTopicFunction, offsetsByPartitionFunction, sizeByPartitionFunction,
        diskByLogDirFunction);

    Broker broker = underTest.get();

    assertThat(broker.getId(), is(0));
    assertThat(broker.getRack(), is("rack"));

    List<LogDir> logDirs = broker.getLogDirs();
    assertThat(logDirs.size(), is(1));
    LogDir logDir = logDirs.get(0);
    assertThat(logDir.getPath(), is("logDir"));
    assertThat(logDir.getDiskFree(), is(123L));
    assertThat(logDir.getDiskTotal(), is(234L));

    List<Topic> topics = logDir.getTopics();
    assertThat(topics.size(), is(1));
    Topic topic = topics.get(0);
    assertThat(topic.getName(), is("topic"));
    assertThat(topic.getRetention(), is(Duration.ofMillis(1)));

    List<PartitionReplica> replicas = topic.getPartitionReplicas();
    assertThat(replicas.size(), is(1));
    PartitionReplica replica = replicas.get(0);
    assertThat(replica.getPartition(), is(0));
    assertThat(replica.isLeader(), is(false));
    assertThat(replica.isInSync(), is(true));
    assertThat(replica.getSizeOnDisk(), is(12L));
    assertThat(replica.getLogSize(), is(42L));
    assertThat(replica.getBeginningOffset(), is(1L));
    assertThat(replica.getEndOffset(), is(3L));
    assertThat(replica.getRecordCount(), is(2L));
  }
}
