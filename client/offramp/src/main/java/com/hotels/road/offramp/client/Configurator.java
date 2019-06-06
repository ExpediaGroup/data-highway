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
package com.hotels.road.offramp.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.HandshakeResponse;

import lombok.RequiredArgsConstructor;

import com.hotels.road.user.agent.MavenUserAgent;
import com.hotels.road.user.agent.UserAgent;
import com.hotels.road.user.agent.UserAgent.Token;

@RequiredArgsConstructor
class Configurator extends ClientEndpointConfig.Configurator {
  private final AtomicReference<List<String>> cookieHolder = new AtomicReference<>();
  private final String username;
  private final String password;

  @Override
  public void beforeRequest(Map<String, List<String>> headers) {
    if (username != null && password != null) {
      String creds = "Basic " + getEncoder().encodeToString((username + ":" + password).getBytes(UTF_8));
      headers.put("Authorization", singletonList(creds));
    }

    List<String> cookie = cookieHolder.get();
    if (cookie != null) {
      headers.put("Cookie", cookie);
    }

    Token token = MavenUserAgent.token(OfframpClient.class, "com.hotels.road", "road-offramp-v2-client");
    String userAgent = UserAgent.create().add(token).toString();
    headers.put("User-Agent", singletonList(userAgent));
  }

  @Override
  public void afterResponse(HandshakeResponse response) {
    List<String> cookie = response.getHeaders().get("set-cookie");
    if (cookie != null) {
      cookieHolder.set(cookie);
    }
  }
}
