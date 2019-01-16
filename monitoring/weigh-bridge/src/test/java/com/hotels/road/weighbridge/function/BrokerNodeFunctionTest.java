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

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.Collection;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.weighbridge.function.BrokerNodeFunction.BrokerNode;

@RunWith(MockitoJUnitRunner.class)
public class BrokerNodeFunctionTest {
  private @Mock AdminClient adminClient;
  private @Mock DescribeClusterResult describeClusterResult;

  private BrokerNodeFunction underTest;

  @Before
  public void before() throws Exception {
    underTest = new BrokerNodeFunction(adminClient);
  }

  @Test
  public void success() throws Exception {
    BrokerNode brokerNode = new BrokerNode(1, "rack", "host");
    Node node = new Node(1, "host", 9092, "rack");
    KafkaFuture<Collection<Node>> kafkaFuture = completedFuture(singleton(node));

    doReturn(describeClusterResult).when(adminClient).describeCluster();
    doReturn(kafkaFuture).when(describeClusterResult).nodes();

    BrokerNode result = underTest.apply(x -> true);

    assertThat(result, is(brokerNode));
  }

  @Test
  public void noRack() throws Exception {
    BrokerNode brokerNode = new BrokerNode(1, "none", "host");
    Node node = new Node(1, "host", 9092, null);
    KafkaFuture<Collection<Node>> kafkaFuture = completedFuture(singleton(node));

    doReturn(describeClusterResult).when(adminClient).describeCluster();
    doReturn(kafkaFuture).when(describeClusterResult).nodes();

    BrokerNode result = underTest.apply(x -> true);

    assertThat(result, is(brokerNode));
  }

  @Test(expected = RuntimeException.class)
  public void notFound() throws Exception {
    Node node = new Node(1, "host", 9092, null);
    KafkaFuture<Collection<Node>> kafkaFuture = completedFuture(singleton(node));

    doReturn(describeClusterResult).when(adminClient).describeCluster();
    doReturn(kafkaFuture).when(describeClusterResult).nodes();

    underTest.apply(x -> false);
  }
}
