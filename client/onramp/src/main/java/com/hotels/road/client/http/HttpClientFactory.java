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

import com.hotels.road.client.OnrampOptions;
import com.hotels.road.client.RoadClient;
import com.hotels.road.user.agent.MavenUserAgent;
import com.hotels.road.user.agent.UserAgent;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.*;

import static java.time.Duration.ofSeconds;

public class HttpClientFactory {
  private static final String ONRAMP_MAVEN_GROUP_ID = "com.hotels.road";
  private static final String ONRAMP_MAVEN_ARTIFACT_ID = "road-onramp-client";

  public static CloseableHttpClient create(HttpClientConnectionManager connectionManager, OnrampOptions options) {
    return org.apache.http.impl.client.HttpClientBuilder
        .create()
        .setUserAgent(userAgent())
        .setConnectionManager(connectionManager)
        .setDefaultRequestConfig(requestConfig())
        .setConnectionReuseStrategy(DefaultClientConnectionReuseStrategy.INSTANCE)
        .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
        .setRetryHandler(getRetryHandler(options))
        .setServiceUnavailableRetryStrategy(new DefaultServiceUnavailableRetryStrategy(1, 500))
        .build();
  }

  private static HttpRequestRetryHandler getRetryHandler(OnrampOptions options) {
    return new RetryHandlerWrapper(options.getRetryBehaviour());
  }

  private static String userAgent() {
    UserAgent.Token token = MavenUserAgent.token(RoadClient.class, ONRAMP_MAVEN_GROUP_ID, ONRAMP_MAVEN_ARTIFACT_ID);
    return UserAgent
        .create()
        .add(token)
        .toString();
  }

  private static RequestConfig requestConfig() {
    return RequestConfig
        .custom()
        .setConnectionRequestTimeout((int) ofSeconds(1).toMillis())
        .setConnectTimeout((int) ofSeconds(6).toMillis())
        .setSocketTimeout((int) ofSeconds(60).toMillis())
        .build();
  }
}
