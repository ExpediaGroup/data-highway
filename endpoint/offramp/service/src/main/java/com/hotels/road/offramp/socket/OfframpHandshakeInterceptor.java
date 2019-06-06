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

import static java.util.stream.Collectors.toSet;

import static com.hotels.road.offramp.model.DefaultOffset.LATEST;

import java.util.Map;
import java.util.Set;

import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriTemplate;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.rest.model.Sensitivity;

@Slf4j
class OfframpHandshakeInterceptor extends HttpSessionHandshakeInterceptor {
  static final String VERSION = "version";
  static final String ROAD_NAME = "roadName";
  static final String STREAM_NAME = "streamName";
  static final String DEFAULT_OFFSET = "defaultOffset";
  static final String GRANTS = "grants";

  private static final UriTemplate URI_TEMPLATE = new UriTemplate(
      "/v{" + VERSION + "}/roads/{" + ROAD_NAME + "}/streams/{" + STREAM_NAME + "}/messages");

  @Override
  public boolean beforeHandshake(
      ServerHttpRequest request,
      ServerHttpResponse response,
      WebSocketHandler wsHandler,
      Map<String, Object> attributes)
    throws Exception {

    log.info("Request received: {} {} - with headers: {}", request.getMethod(), request.getURI(), request.getHeaders());

    Map<String, String> segments = URI_TEMPLATE.match(request.getURI().getPath());
    attributes.put(VERSION, segments.get(VERSION));
    attributes.put(ROAD_NAME, segments.get(ROAD_NAME));
    attributes.put(STREAM_NAME, segments.get(STREAM_NAME));

    MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(request.getURI()).build().getQueryParams();

    DefaultOffset defaultOffset = Mono
        .justOrEmpty(parameters.getFirst(DEFAULT_OFFSET))
        .map(DefaultOffset::valueOf)
        .defaultIfEmpty(LATEST)
        .block();
    attributes.put(DEFAULT_OFFSET, defaultOffset);

    Set<Sensitivity> grants = Mono
        .justOrEmpty(parameters.get(GRANTS))
        .flatMapMany(Flux::fromIterable)
        .filter(s -> !s.isEmpty())
        .map(Sensitivity::valueOf)
        .collect(toSet())
        .block();
    attributes.put(GRANTS, grants);

    return super.beforeHandshake(request, response, wsHandler, attributes);
  }
}
