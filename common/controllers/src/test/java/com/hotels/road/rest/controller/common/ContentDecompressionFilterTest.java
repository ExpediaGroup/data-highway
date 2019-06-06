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
package com.hotels.road.rest.controller.common;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import javax.servlet.FilterChain;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpHeaders;

@RunWith(Parameterized.class)
public class ContentDecompressionFilterTest {
  public static final byte[] TEST_BYTES;
  public static final byte[] GZIP_BYTES;
  public static final byte[] DEFLATE_BYTES;

  static {
    Random r = new Random();
    TEST_BYTES = new byte[1000];
    r.nextBytes(TEST_BYTES);

    ByteArrayOutputStream baos;
    baos = new ByteArrayOutputStream();
    try (GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(baos)) {
      gzos.write(TEST_BYTES);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    GZIP_BYTES = baos.toByteArray();

    baos = new ByteArrayOutputStream();
    try (DeflateCompressorOutputStream gzos = new DeflateCompressorOutputStream(baos)) {
      gzos.write(TEST_BYTES);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    DEFLATE_BYTES = baos.toByteArray();
  }

  public HttpServletRequest request = mock(HttpServletRequest.class);
  public HttpServletResponse response = mock(HttpServletResponse.class);
  public FilterChain chain = mock(FilterChain.class);

  public ContentDecompressionFilter filter = new ContentDecompressionFilter();

  @Parameters
  public static Collection<Object[]> data() {
    return asList(
        new Object[] { "gzip", GZIP_BYTES },
        new Object[] { "deflate", DEFLATE_BYTES },
        new Object[] { null, TEST_BYTES });
  }

  public @Parameter(0) String contentEncoding;
  public @Parameter(1) byte[] inputBytes;

  @Test
  public void test() throws Exception {
    when(request.getHeader(HttpHeaders.CONTENT_ENCODING)).thenReturn(contentEncoding);
    when(request.getInputStream()).thenReturn(new ServletInputStreamWrapper(new ByteArrayInputStream(inputBytes)));

    ArgumentCaptor<HttpServletRequest> requestCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);
    doNothing().when(chain).doFilter(requestCaptor.capture(), any(ServletResponse.class));

    filter.doFilter(request, response, chain);

    HttpServletRequest outputRequest = requestCaptor.getValue();
    byte[] outputBytes = toByteArray(outputRequest.getInputStream());

    assertTrue(Arrays.equals(outputBytes, TEST_BYTES));
  }

  private byte[] toByteArray(InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int n = 0;
    while (-1 != (n = input.read(buffer))) {
      output.write(buffer, 0, n);
    }
    return output.toByteArray();
  }

}
