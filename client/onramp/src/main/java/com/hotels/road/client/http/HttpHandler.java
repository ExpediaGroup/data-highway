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
package com.hotels.road.client.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

import static lombok.AccessLevel.PACKAGE;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Optional;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.IdleConnectionEvictor;

import lombok.Getter;

import com.hotels.road.client.OnrampOptions;

public class HttpHandler implements AutoCloseable {
  @Getter(PACKAGE)
  private final URI url;
  private final Optional<String> creds;

  private final CloseableHttpClient client;
  private final IdleConnectionEvictor connectionEvictor;

  HttpHandler(URI url, OnrampOptions options) {
    this.url = url;

    String username = options.getUsername();
    String password = options.getPassword();
    if (username != null && password != null) {
      creds = Optional.of(Base64.getEncoder().encodeToString((username + ":" + password).getBytes(UTF_8)));
    } else {
      creds = Optional.empty();
    }

    HttpClientConnectionManager connectionManager = HttpClientConnectionManagerFactory.create(options.getThreads(),
        options.getTlsConfigFactory());

    client = HttpClientFactory.create(connectionManager, options);

    connectionEvictor = new IdleConnectionEvictor(connectionManager, 1, SECONDS, 5, SECONDS);
    connectionEvictor.start();
  }

  public HttpResponse get(String path) throws IOException {
    HttpGet get = new HttpGet(url.resolve(path));
    creds.ifPresent(x -> get.setHeader("Authorization", "Basic " + x));
    return client.execute(get);
  }

  public HttpResponse post(String path, HttpEntity entity) throws IOException {
    HttpPost post = new HttpPost(url.resolve(path));
    creds.ifPresent(x -> post.setHeader("Authorization", "Basic " + x));
    post.setEntity(entity);
    return client.execute(post);
  }

  @Override
  public void close() throws Exception {
    connectionEvictor.shutdown();
    client.close();
  }

  public static HttpHandler paver(OnrampOptions options) {
    return new HttpHandler(createUri(options.getHost(), "paver", "v1"), options);
  }

  public static HttpHandler onramp(OnrampOptions options) {
    return new HttpHandler(createUri(options.getHost(), "onramp", "v1"), options);
  }

  private static URI createUri(String host, String app, String version) {
    return URI.create(String.format("https://%s/%s/%s/", host, app, version));
  }
}
