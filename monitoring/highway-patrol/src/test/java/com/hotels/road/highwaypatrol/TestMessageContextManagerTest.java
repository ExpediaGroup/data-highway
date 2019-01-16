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
package com.hotels.road.highwaypatrol;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestMessageContextManagerTest {
  private final String origin = "test";
  private @Mock ScheduledExecutorService contextWorkerService;
  private @Mock MessagesMetricSet metrics;
  private @Mock TestMessageContext context;

  private TestMessageContextManager underTest;

  @Before
  public void before() throws Exception {
    underTest = new TestMessageContextManager(contextWorkerService, metrics, origin, 10, 4096);
  }

  @Test
  public void context_is_in_activeMessages() throws Exception {
    TestMessageContext nextContext = underTest.nextContext();
    assertThat(underTest.getContext(nextContext.getMessage()), is(nextContext));
  }

  @Test
  public void context_is_removed_from_active_when_lookedup() throws Exception {
    TestMessageContext nextContext = underTest.nextContext();
    assertThat(underTest.getContext(nextContext.getMessage()), is(nextContext));
    assertThat(underTest.getContext(nextContext.getMessage()), nullValue());
  }

  @Test
  public void context_is_linked_to_previous_in_same_group() throws Exception {
    Map<Integer, TestMessageContext> previousContexts = new HashMap<>();
    for (int i = 0; i < 10_000; i++) {
      TestMessageContext nextContext = underTest.nextContext();
      assertThat(previousContexts.get(nextContext.getMessage().getGroup()), is(nextContext.getPrevious()));
      previousContexts.put(nextContext.getMessage().getGroup(), nextContext);
    }
  }
}
