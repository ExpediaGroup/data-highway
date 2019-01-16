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

import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.google.common.io.ByteSource.wrap;
import static com.google.common.io.Files.asByteSink;

import java.io.File;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction.Replica;

public class SizeByPartitionFunctionTest {
  public @Rule TemporaryFolder temp = new TemporaryFolder();

  private final SizeByPartitionFunction underTest = new SizeByPartitionFunction();

  @Test
  public void typical() throws Exception {
    TopicPartition topicPartition = new TopicPartition("topic", 0);
    File folder = temp.newFolder(topicPartition.toString());
    wrap(new byte[] { 0 }).copyTo(asByteSink(new File(folder, "data")));
    Map<TopicPartition, Replica> partitionsAndLogDir = singletonMap(topicPartition,
        new Replica(temp.getRoot().getAbsolutePath(), 0L));

    Map<TopicPartition, Long> result = underTest.apply(partitionsAndLogDir);

    assertThat(result.size(), is(1));
    long size = result.get(topicPartition);
    assertThat(size, is(1L));
  }
}
