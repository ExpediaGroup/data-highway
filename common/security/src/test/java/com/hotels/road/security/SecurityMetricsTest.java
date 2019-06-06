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
package com.hotels.road.security;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static com.hotels.road.security.AuthenticationType.AUTHENTICATED;
import static com.hotels.road.security.AuthorisationOutcome.AUTHORISED;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

@RunWith(MockitoJUnitRunner.class)
public class SecurityMetricsTest {
  private @Mock MeterRegistry registry;

  private SecurityMetrics underTest;

  @Before
  public void before() throws Exception {
    underTest = new SecurityMetrics(registry, "counterName");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testName() throws Exception {
    Counter counter = mock(Counter.class);
    doReturn(counter).when(registry).counter(eq("counterName"), any(Iterable.class));

    underTest.increment("road", AUTHENTICATED, AUTHORISED);

    ArgumentCaptor<Iterable<Tag>> captor = ArgumentCaptor.forClass(Iterable.class);
    verify(registry).counter(eq("counterName"), captor.capture());
    verify(counter).increment();

    Iterable<Tag> tags = captor.getValue();
    assertThat(tags, hasItems(Tag.of("road", "road"), Tag.of("authentication", "AUTHENTICATED"),
        Tag.of("authorisation", "AUTHORISED")));
  }
}
