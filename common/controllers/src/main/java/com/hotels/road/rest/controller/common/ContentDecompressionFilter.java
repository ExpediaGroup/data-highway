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
package com.hotels.road.rest.controller.common;

import static org.springframework.core.Ordered.LOWEST_PRECEDENCE;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

@Order(LOWEST_PRECEDENCE)
@Component
public class ContentDecompressionFilter implements Filter {
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
    throws IOException, ServletException {
    if (request instanceof HttpServletRequest && response instanceof HttpServletResponse) {
      HttpServletRequest httpRequest = (HttpServletRequest) request;

      String contentEncoding = httpRequest.getHeader(HttpHeaders.CONTENT_ENCODING);

      request = new HttpServletRequestWrapper(httpRequest) {
        @Override
        public ServletInputStream getInputStream() throws IOException {
          ServletInputStream originalStream = httpRequest.getInputStream();
          ServletInputStream newStream;

          if ("gzip".equals(contentEncoding)) {
            newStream = new ServletInputStreamWrapper(new GzipCompressorInputStream(originalStream));
          } else if ("deflate".equals(contentEncoding)) {
            newStream = new ServletInputStreamWrapper(new DeflateCompressorInputStream(originalStream));
          } else {
            newStream = originalStream;
          }

          return newStream;
        }
      };
    }

    chain.doFilter(request, response);
  }

  @Override
  public void destroy() {}
}
