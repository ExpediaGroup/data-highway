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
package com.hotels.road.user.agent;

import java.io.IOException;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.util.UriTemplate;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public class UserAgentMetricFilter implements Filter {
  private static final UriTemplate template = new UriTemplate("/roads/{name}/");
  private final @NonNull MeterRegistry registry;
  private final @NonNull Set<String> products;

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
    throws IOException, ServletException {
    if (request instanceof HttpServletRequest) {
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      String roadName = template.match(httpRequest.getRequestURI()).get("name");
      UserAgent userAgent = UserAgent.parse(httpRequest.getHeader("User-Agent"));
      products.forEach(product -> {
        userAgent.token(product).ifPresent(token -> {
          String version = token.getVersion().replaceAll("\\.", "-");
          Tag roadTag = Tag.of("road", roadName);
          Tag productTag = Tag.of("product", product);
          Tag versionTag = Tag.of("version", version);
          ImmutableList<Tag> tags = ImmutableList.of(roadTag, productTag, versionTag);
          registry.counter("user-agent-metric-filter", tags).increment();
        });
      });
    }
    chain.doFilter(request, response);
  }

  @Override
  public void init(FilterConfig config) throws ServletException {}

  @Override
  public void destroy() {}
}
