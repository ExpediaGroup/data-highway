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
package com.hotels.road.user.agent;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.Silent.class)
public class UserAgentMetricFilterTest {

  private final MeterRegistry registry = new SimpleMeterRegistry();
  private @Mock HttpServletRequest request;
  private @Mock ServletResponse response;
  private @Mock FilterChain chain;

  private static final Iterable<Tag> tags1 = ImmutableList.of(Tag.of("road", "road1"), Tag.of("product", "product1"),
      Tag.of("version", "1-0"));
  private static final Iterable<Tag> tags2 = ImmutableList.of(Tag.of("road", "road1"), Tag.of("product", "product2"),
      Tag.of("version", "2-0"));
  private static final Iterable<Tag> tags3 = ImmutableList.of(Tag.of("road", "road1"), Tag.of("product", "product3"),
      Tag.of("version", "3-0"));
  private static final Iterable<Tag> tags4 = ImmutableList.of(Tag.of("road", "road1"), Tag.of("product", "product4"),
      Tag.of("version", "4-0"));
  private final Counter counter1 = registry.counter("user-agent-metric-filter", tags1);
  private final Counter counter2 = registry.counter("user-agent-metric-filter", tags2);
  private final Counter counter3 = registry.counter("user-agent-metric-filter", tags3);
  private final Counter counter4 = registry.counter("user-agent-metric-filter", tags4);

  @Test
  public void testName() throws Exception {
    when(request.getRequestURI()).thenReturn("/onramp/v1/roads/road1/messages");
    when(request.getHeader("User-Agent")).thenReturn("product1/1.0 product2/2.0 product3/3.0");
    Set<String> products = new HashSet<>(Arrays.asList("product1", "product2", "product4"));
    Filter underTest = new UserAgentMetricFilter(registry, products);

    underTest.doFilter(request, response, chain);

    assertThat(counter1.count(), is(1.0));
    assertThat(counter2.count(), is(1.0));
    assertThat(counter3.count(), is(0.0));
    assertThat(counter4.count(), is(0.0));
    verify(chain).doFilter(request, response);
  }
}
