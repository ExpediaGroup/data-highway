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
package com.hotels.road.s3.io;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class AsyncHandlerTest {

  private final ExecutorService executor = Executors.newFixedThreadPool(2);

  private AsyncHandler<Integer> underTest;

  @Before
  public void before() {
    underTest = new AsyncHandler<>(executor);
  }

  @Test
  public void happyPath() {
    underTest.supply(() -> 1);
    try {
      underTest.checkForFailures();
    } catch (Exception e) {
      fail();
    }
    List<Integer> result = underTest.waitForCompletion();

    assertThat(result, is(ImmutableList.of(1)));
  }

  @Test(expected = RuntimeException.class)
  public void exceptionOnCheck() throws InterruptedException {
    underTest.supply(() -> {
      throw new RuntimeException();
    });
    Thread.sleep(100L);
    underTest.checkForFailures();
  }

  @Test(expected = RuntimeException.class)
  public void exceptionOnCompletion() {
    underTest.supply(() -> {
      throw new RuntimeException();
    });
    underTest.waitForCompletion();
  }

  @Test(expected = CompletionException.class, timeout = 100L)
  public void failFast() {
    // succeed after 5 seconds
    underTest.supply(() -> {
      try {
        Thread.sleep(5000L);
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      return 1;
    });
    // fail immediately
    underTest.supply(() -> {
      throw new RuntimeException();
    });

    underTest.waitForCompletion();
  }

}
