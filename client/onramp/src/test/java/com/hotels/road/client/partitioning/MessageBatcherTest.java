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
package com.hotels.road.client.partitioning;

import static java.util.stream.Collectors.toList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Test;

public class MessageBatcherTest {
  @Test(timeout = 1000)
  public void batcher_batches_upto_maxBatchSize_under_pressure() throws Exception {
    List<List<Integer>> batches = new ArrayList<>();
    Function<List<Integer>, List<Integer>> batchHandler = l -> {
      batches.add(l);
      return l.stream().map(i -> i * 10).collect(toList());
    };
    AtomicReference<Runnable> processQueue = new AtomicReference<>();
    ThreadFactory factory = r -> {
      processQueue.set(r);
      return new Thread();
    };
    Thread t = null;
    try (MessageBatcher<Integer, Integer> batcher = new MessageBatcher<>(5, 2, EnqueueBehaviour.COMPLETE_EXCEPTIONALLY,
        batchHandler, factory)) {
      CompletableFuture<Integer> result1 = batcher.apply(1);
      CompletableFuture<Integer> result2 = batcher.apply(2);
      CompletableFuture<Integer> result3 = batcher.apply(3);
      CompletableFuture<Integer> result4 = batcher.apply(4);
      CompletableFuture<Integer> result5 = batcher.apply(5);

      t = new Thread(processQueue.get());
      t.start();

      assertThat(result1.get(), is(10));
      assertThat(result2.get(), is(20));
      assertThat(result3.get(), is(30));
      assertThat(result4.get(), is(40));
      assertThat(result5.get(), is(50));
    }
    t.join();

    assertThat(batches.size(), is(3));
    assertThat(batches.get(0).size(), is(2));
    assertThat(batches.get(0), contains(1, 2));
    assertThat(batches.get(1).size(), is(2));
    assertThat(batches.get(1), contains(3, 4));
    assertThat(batches.get(2).size(), is(1));
    assertThat(batches.get(2), contains(5));
  }

  @Test
  public void messages_fail_when_buffer_full() throws Exception {
    Function<List<Integer>, List<Integer>> batchHandler = Function.identity();
    AtomicReference<Runnable> processQueue = new AtomicReference<>();
    ThreadFactory factory = r -> {
      processQueue.set(r);
      return new Thread();
    };
    Thread t = null;
    try (MessageBatcher<Integer, Integer> batcher = new MessageBatcher<>(4, 2, EnqueueBehaviour.COMPLETE_EXCEPTIONALLY,
        batchHandler, factory)) {
      CompletableFuture<Integer> result1 = batcher.apply(1);
      CompletableFuture<Integer> result2 = batcher.apply(2);
      CompletableFuture<Integer> result3 = batcher.apply(3);
      CompletableFuture<Integer> result4 = batcher.apply(4);
      CompletableFuture<Integer> result5 = batcher.apply(5);

      t = new Thread(processQueue.get());
      t.start();

      assertThat(result1.get(), is(1));
      assertThat(result2.get(), is(2));
      assertThat(result3.get(), is(3));
      assertThat(result4.get(), is(4));

      try {
        result5.get();
        fail();
      } catch (ExecutionException e) {
        assertTrue(e.getCause() instanceof IllegalStateException);
      }
    }
    t.join();
  }

  @Test(timeout = 1000)
  public void batcher_blocks_when_queue_is_full() throws Exception {
    Function<List<Integer>, List<Integer>> batchHandler = Function.identity();
    AtomicReference<Runnable> processQueue = new AtomicReference<>();
    ThreadFactory factory = r -> {
      processQueue.set(r);
      return new Thread();
    };

    CountDownLatch latch = new CountDownLatch(1);

    Thread thread = null;
    try (MessageBatcher<Integer, Integer> batcher = new MessageBatcher<>(4, 2, EnqueueBehaviour.BLOCK_AND_WAIT,
        batchHandler, factory)) {
      thread = new Thread(() -> {
        batcher.apply(1);
        batcher.apply(2);
        batcher.apply(3);
        batcher.apply(4);
        latch.countDown();
        batcher.apply(5);
      });
      thread.start();
      latch.await();

      while (thread.getState() != State.WAITING) {}
    } finally {
      if (thread != null) {
        thread.interrupt();
        thread.join();
      }
    }
  }

  @Test
  public void batcher_passes_exceptions_up() throws Exception {
    Function<List<Integer>, List<Integer>> batchHandler = l -> {
      throw new RuntimeException("exception message");
    };
    try (MessageBatcher<Integer, Integer> batcher = new MessageBatcher<>(5, 2, EnqueueBehaviour.COMPLETE_EXCEPTIONALLY,
        batchHandler)) {
      batcher.apply(1).get();
      fail("Shouldn't get here");
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RuntimeException.class));
      assertThat(e.getCause().getMessage(), is("exception message"));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("resource")
  public void maxBatch_must_fit_in_buffer() throws Exception {
    new MessageBatcher<>(2, 3, EnqueueBehaviour.COMPLETE_EXCEPTIONALLY, Function.identity());
  }
}
