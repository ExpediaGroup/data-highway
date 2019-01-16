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
package com.hotels.road.offramp.client;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class ConfiguratorTest {
  private final Configurator underTest = new Configurator("username", "password");

  @Test
  public void auth() throws Exception {
    Map<String, List<String>> headers = new HashMap<>();
    underTest.beforeRequest(headers);
    assertThat(headers.get("Authorization"), is(singletonList("Basic dXNlcm5hbWU6cGFzc3dvcmQ=")));
  }

  @Test
  public void cookie() throws Exception {
    Map<String, List<String>> headers = new HashMap<>();
    underTest.beforeRequest(headers);
    assertThat(headers.get("Cookie"), is(nullValue()));

    underTest.afterResponse(() -> singletonMap("set-cookie", singletonList("k=v")));

    underTest.beforeRequest(headers);
    assertThat(headers.get("Cookie"), is(singletonList("k=v")));
  }

  @Test
  public void newCookie() throws Exception {
    underTest.afterResponse(() -> singletonMap("set-cookie", singletonList("k=v1")));

    Map<String, List<String>> headers = new HashMap<>();
    underTest.beforeRequest(headers);
    assertThat(headers.get("Cookie"), is(singletonList("k=v1")));

    underTest.afterResponse(() -> singletonMap("set-cookie", singletonList("k=v2")));

    underTest.beforeRequest(headers);
    assertThat(headers.get("Cookie"), is(singletonList("k=v2")));
  }

  @Test
  public void userAgent() throws Exception {
    Map<String, List<String>> headers = new HashMap<>();
    underTest.beforeRequest(headers);
    assertThat(headers.get("User-Agent").get(0), startsWith("road-offramp-v2-client/"));
  }
}
