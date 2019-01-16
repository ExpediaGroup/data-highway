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
package com.hotels.road.security;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class RemoteAddressFilter implements Filter {
  private static final String X_FORWARDED_FOR = "x-forwarded-for";

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
    throws IOException, ServletException {
    if (request instanceof HttpServletRequest) {
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      request = new HttpServletRequestWrapper(httpRequest) {
        @Override
        public String getRemoteAddr() {
          String xForwardedFor = httpRequest.getHeader(X_FORWARDED_FOR);
          if (xForwardedFor != null) {
            log.debug("Request included {} address: {}", X_FORWARDED_FOR, xForwardedFor);
            return xForwardedFor;
          }
          String remoteAddr = httpRequest.getRemoteAddr();
          log.debug("Request did not include {} address, falling back to remote address: {}", X_FORWARDED_FOR,
              remoteAddr);
          return remoteAddr;
        }
      };
    }
    chain.doFilter(request, response);
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void destroy() {}
}
