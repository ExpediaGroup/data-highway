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
package com.hotels.road.loadingbay.lander.kubernetes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.loadingbay.lander.Lander;
import com.hotels.road.loadingbay.lander.LanderConfiguration;
import com.hotels.road.loadingbay.lander.OffsetRange;

@RunWith(MockitoJUnitRunner.class)
public class ParallelKubernetesLanderTest {

  private @Mock Lander.Factory delegate;
  private @Mock Lander lander1;
  private @Mock Lander lander2;
  private @Mock Lander lander3;

  private final OffsetRange range = new OffsetRange(0, 1L);
  private final Map<Integer, OffsetRange> offsets = ImmutableMap.of(0, range, 1, range, 2, range);
  private final LanderConfiguration config = new LanderConfiguration("roadName", "topicName", offsets, "s3KeyPrefix",
      false, "partitionColumnValue", false);

  private final CompletableFuture<LanderConfiguration> future1 = new CompletableFuture<>();
  private final CompletableFuture<LanderConfiguration> future2 = new CompletableFuture<>();
  private final CompletableFuture<LanderConfiguration> future3 = new CompletableFuture<>();

  private Lander.Factory underTestFactory;
  ParallelKubernetesLander underTest;

  @Before
  public void before() throws Exception {
    doReturn(lander1, lander2, lander3).when(delegate).newInstance(any());
    doReturn(future1).when(lander1).run();
    doReturn(future2).when(lander2).run();
    doReturn(future3).when(lander3).run();

    underTestFactory = new ParallelKubernetesLander.Factory(delegate, 1);
    underTest = (ParallelKubernetesLander) underTestFactory.newInstance(config);
  }

  @Test
  public void success() throws Exception {
    future1.complete(config.withOffsets(ImmutableMap.of(0, range)));
    future2.complete(config.withOffsets(ImmutableMap.of(1, range)));
    future3.complete(config.withOffsets(ImmutableMap.of(2, range)));

    LanderConfiguration result = underTest.run().join();

    assertThat(result, is(config));
  }

  @Test
  public void failFast() throws Exception {
    future1.completeExceptionally(new Exception());
    future2.completeExceptionally(new Exception());

    try {
      underTest.run().join();
      fail();
    } catch (Exception ignore) {}

    assertThat(future1.isCancelled(), is(false));
    assertThat(future2.isCancelled(), is(false));
    assertThat(future3.isCancelled(), is(true));
  }
}
