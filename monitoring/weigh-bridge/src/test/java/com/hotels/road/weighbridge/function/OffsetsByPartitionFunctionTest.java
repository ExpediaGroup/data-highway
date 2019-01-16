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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.weighbridge.function.OffsetsByPartitionFunction.Offsets;

@RunWith(MockitoJUnitRunner.class)
public class OffsetsByPartitionFunctionTest {
  private @Mock KafkaConsumer<?, ?> kafkaConsumer;

  private OffsetsByPartitionFunction underTest;

  @Before
  public void before() throws Exception {
    underTest = new OffsetsByPartitionFunction(kafkaConsumer);
  }

  @Test
  public void typical() throws Exception {
    TopicPartition topicPartition = new TopicPartition("topic", 0);
    Collection<TopicPartition> topicPartitions = singleton(topicPartition);

    doReturn(singletonMap(topicPartition, 1L)).when(kafkaConsumer).beginningOffsets(topicPartitions);
    doReturn(singletonMap(topicPartition, 3L)).when(kafkaConsumer).endOffsets(topicPartitions);

    Map<TopicPartition, Offsets> result = underTest.apply(topicPartitions);

    assertThat(result.size(), is(1));
    Offsets offsets = result.get(topicPartition);
    assertThat(offsets.getBeginning(), is(1L));
    assertThat(offsets.getEnd(), is(3L));
    assertThat(offsets.getCount(), is(2L));
  }
}
