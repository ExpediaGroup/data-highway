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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hotels.road.weighbridge.function.DiskByLogDirFunction.Disk;
import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction.Replica;

public class DiskByLogDirFunctionTest {
  public @Rule TemporaryFolder temp = new TemporaryFolder();

  DiskByLogDirFunction underTest = new DiskByLogDirFunction();

  @Test
  public void typical() throws Exception {
    String logDir = temp.getRoot().getAbsolutePath();
    TopicPartition topicPartition = new TopicPartition("topic", 0);
    Map<TopicPartition, Replica> replicaByPartition = Collections.singletonMap(topicPartition, new Replica(logDir, 0L));

    Map<String, Disk> result = underTest.apply(replicaByPartition);

    assertThat(result.size(), is(1));
    Disk disk = result.get(logDir);
    assertThat(disk.getFree() > 0L, is(true));
    assertThat(disk.getTotal() > 0L, is(true));
  }
}
