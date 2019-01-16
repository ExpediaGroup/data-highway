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
package com.hotels.road.client.http;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.Test;

public class HttpClientConnectionManagerFactoryTest {
  @Test
  public void testWithThreadsConfiguration() throws Exception {
    HttpClientConnectionManager connectionManager = HttpClientConnectionManagerFactory.create(3, null);

    assertThat(connectionManager, is(instanceOf(PoolingHttpClientConnectionManager.class)));

    PoolingHttpClientConnectionManager poolingConnectionManager = (PoolingHttpClientConnectionManager) connectionManager;
    assertThat(poolingConnectionManager.getDefaultMaxPerRoute(), is(9));
    assertThat(poolingConnectionManager.getMaxTotal(), is(9));
    assertThat(poolingConnectionManager.getValidateAfterInactivity(), is(1000));
  }
}
