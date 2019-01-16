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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoteAddressFilterTest {
  @Mock
  private HttpServletRequest request;
  @Mock
  private ServletResponse response;
  @Mock
  private FilterChain chain;
  @Captor
  private ArgumentCaptor<HttpServletRequest> captor;

  private final RemoteAddressFilter underTest = new RemoteAddressFilter();

  @Test
  public void notAnHttpServletRequest() throws Exception {
    ServletRequest request = mock(ServletRequest.class);

    underTest.doFilter(request, response, chain);

    verify(chain).doFilter(request, response);
  }

  @Test
  public void prioritiseXForwardedForHeader() throws Exception {
    underTest.doFilter(request, response, chain);

    verify(chain).doFilter(captor.capture(), eq(response));

    HttpServletRequest captured = captor.getValue();

    when(request.getHeader("x-forwarded-for")).thenReturn("1.2.3.4");

    assertThat(captured.getRemoteAddr(), is("1.2.3.4"));
  }

  @Test
  public void fallbackToRemoteAddress() throws Exception {
    underTest.doFilter(request, response, chain);

    verify(chain).doFilter(captor.capture(), eq(response));

    HttpServletRequest captured = captor.getValue();

    when(request.getRemoteAddr()).thenReturn("4.3.2.1");

    assertThat(captured.getRemoteAddr(), is("4.3.2.1"));
  }
}
