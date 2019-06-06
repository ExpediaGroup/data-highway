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
package com.hotels.road.offramp.socket;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.hotels.road.offramp.model.DefaultOffset.EARLIEST;
import static com.hotels.road.offramp.model.DefaultOffset.LATEST;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.DEFAULT_OFFSET;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.GRANTS;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.ROAD_NAME;
import static com.hotels.road.offramp.socket.OfframpHandshakeInterceptor.STREAM_NAME;
import static com.hotels.road.rest.model.Sensitivity.PII;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.server.ServerHttpRequest;

@RunWith(MockitoJUnitRunner.class)
public class OfframpHandshakeInterceptorTest {

  private @Mock ServerHttpRequest request;

  private final OfframpHandshakeInterceptor underTest = new OfframpHandshakeInterceptor();

  @Test
  public void allParameters() throws Exception {
    when(request.getURI()).thenReturn(
        URI.create("ws://localhost/offramp/v2/roads/road1/streams/stream1/messages?defaultOffset=EARLIEST&grants=PII"));

    Map<String, Object> attributes = new HashMap<>();
    underTest.beforeHandshake(request, null, null, attributes);

    assertThat(attributes.get(ROAD_NAME), is("road1"));
    assertThat(attributes.get(STREAM_NAME), is("stream1"));
    assertThat(attributes.get(DEFAULT_OFFSET), is(EARLIEST));
    assertThat(attributes.get(GRANTS), is(singleton(PII)));
  }

  @Test
  public void missingDefaultOffset() throws Exception {
    when(request.getURI())
        .thenReturn(URI.create("ws://localhost/offramp/v2/roads/road1/streams/stream1/messages?grants=PII"));

    Map<String, Object> attributes = new HashMap<>();
    underTest.beforeHandshake(request, null, null, attributes);

    assertThat(attributes.get(ROAD_NAME), is("road1"));
    assertThat(attributes.get(STREAM_NAME), is("stream1"));
    assertThat(attributes.get(DEFAULT_OFFSET), is(LATEST));
    assertThat(attributes.get(GRANTS), is(singleton(PII)));
  }

  @Test
  public void missingGrants() throws Exception {
    when(request.getURI()).thenReturn(
        URI.create("ws://localhost/offramp/v2/roads/road1/streams/stream1/messages?defaultOffset=EARLIEST"));

    Map<String, Object> attributes = new HashMap<>();
    underTest.beforeHandshake(request, null, null, attributes);

    assertThat(attributes.get(ROAD_NAME), is("road1"));
    assertThat(attributes.get(STREAM_NAME), is("stream1"));
    assertThat(attributes.get(DEFAULT_OFFSET), is(EARLIEST));
    assertThat(attributes.get(GRANTS), is(emptySet()));
  }

  @Test
  public void emptyGrants() throws Exception {
    when(request.getURI()).thenReturn(
        URI.create("ws://localhost/offramp/v2/roads/road1/streams/stream1/messages?defaultOffset=EARLIEST&grants="));

    Map<String, Object> attributes = new HashMap<>();
    underTest.beforeHandshake(request, null, null, attributes);

    assertThat(attributes.get(ROAD_NAME), is("road1"));
    assertThat(attributes.get(STREAM_NAME), is("stream1"));
    assertThat(attributes.get(DEFAULT_OFFSET), is(EARLIEST));
    assertThat(attributes.get(GRANTS), is(emptySet()));
  }
}
