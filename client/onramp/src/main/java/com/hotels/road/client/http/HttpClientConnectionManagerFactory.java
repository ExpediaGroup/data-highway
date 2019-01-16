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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.hotels.road.tls.TLSConfig;

public class HttpClientConnectionManagerFactory {
  private static final int MAX_CONNECTIONS_MULTIPLIER = 3;

  public static HttpClientConnectionManager create(int threads, TLSConfig.Factory tlsConfigFactory) {
    return createClientConnectionManager(createRegistry(tlsConfigFactory), threads);
  }

  private static Registry<ConnectionSocketFactory> createRegistry(TLSConfig.Factory tlsConfigFactory) {
    RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
    TLSConfig tlsConfig = tlsConfigFactory == null ? null : tlsConfigFactory.create();
    if (tlsConfig != null) {
      SSLContext sslContext = tlsConfig.getSslContext();
      HostnameVerifier hostnameVerifier = tlsConfig.getHostnameVerifier();
      SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
      registryBuilder.register("https", socketFactory);
    } else {
      registryBuilder.register("https", SSLConnectionSocketFactory.getSocketFactory());
    }
    return registryBuilder.build();
  }

  private static PoolingHttpClientConnectionManager createClientConnectionManager(
      Registry<ConnectionSocketFactory> registry,
      int threads) {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
    connectionManager.setDefaultMaxPerRoute(threads * MAX_CONNECTIONS_MULTIPLIER);
    connectionManager.setMaxTotal(threads * MAX_CONNECTIONS_MULTIPLIER);
    connectionManager.setValidateAfterInactivity(1000);
    return connectionManager;
  }
}
