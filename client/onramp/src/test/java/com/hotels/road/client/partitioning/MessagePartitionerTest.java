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
package com.hotels.road.client.partitioning;

import static java.util.Collections.emptyList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

public class MessagePartitionerTest {
  @Test
  public void partitionTest() throws Exception {
    Supplier<Function<Integer, Integer>> hasherSupplier = () -> i -> i % 10;
    List<Integer> partition1 = new ArrayList<>();
    List<Integer> partition2 = new ArrayList<>();
    Function<Integer, Integer> client1 = i -> {
      partition1.add(i);
      return i * 2;
    };
    Function<Integer, Integer> client2 = i -> {
      partition2.add(i);
      return i * 2;
    };
    List<Function<Integer, Integer>> delegates = Arrays.asList(client1, client2);
    try (MessagePartitioner<Integer, Integer> partitioner = new MessagePartitioner<>(hasherSupplier, delegates)) {
      for (int i = 0; i < 20; i++) {
        assertThat(partitioner.apply(i), is(i * 2));
      }
      assertThat(partition1.size() + partition2.size(), is(20));
      for (int i = 0; i < 10; i++) {
        if (partition1.contains(i)) {
          assertThat(partition1.contains(i + 10), is(true));
          assertThat(partition2.contains(i), is(false));
          assertThat(partition2.contains(i + 10), is(false));
        }
        if (partition2.contains(i)) {
          assertThat(partition2.contains(i + 10), is(true));
          assertThat(partition1.contains(i), is(false));
          assertThat(partition1.contains(i + 10), is(false));
        }
      }
    }
  }

  @Test
  public void hasherThrowsExceptionWhichIsPropogated() throws Exception {
    Supplier<Function<Integer, Integer>> hasherSupplier = () -> i -> {
      throw new RuntimeException(Integer.toString(i));
    };
    List<Function<Integer, Integer>> delegates = Arrays.asList(Function.identity());
    try (MessagePartitioner<Integer, Integer> partitioner = new MessagePartitioner<>(hasherSupplier, delegates)) {
      try {
        partitioner.apply(1);
        fail("Should throw exception before reaching this point");
      } catch (RuntimeException e) {
        assertThat(e.getMessage(), is("1"));
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void havingNoDelegatesIsAnError() throws Exception {
    try (MessagePartitioner<Integer, Integer> partitioner = new MessagePartitioner<>(() -> i -> 1, emptyList())) {}
  }

  @Test
  public void delegateExceptionIsPropogated() throws Exception {
    List<Function<Integer, Integer>> delegates = Arrays.asList(i -> {
      throw new RuntimeException(Integer.toString(i));
    });
    try (MessagePartitioner<Integer, Integer> partitioner = new MessagePartitioner<>(() -> i -> 1, delegates)) {
      try {
        partitioner.apply(1);
        fail("Should throw exception before reaching this point");
      } catch (RuntimeException e) {
        assertThat(e.getMessage(), is("1"));
      }
    }
  }
}
