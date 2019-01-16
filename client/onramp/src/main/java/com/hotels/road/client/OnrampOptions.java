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
package com.hotels.road.client;

import static lombok.AccessLevel.PACKAGE;

import java.io.Serializable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.HostnameVerifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hotels.road.tls.TLSConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Wither;

@Value
@AllArgsConstructor(access = PACKAGE)
@Builder(builderClassName = "Builder")
@Wither
public class OnrampOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  String username;
  String password;
  @NonNull String host;
  @NonNull String roadName;
  int threads;
  @NonNull RetryHandler retryBehaviour;
  @NonNull ObjectMapper objectMapper;
  TLSConfig.Factory tlsConfigFactory;

  public static class Builder {
    private String username;
    private String password;
    private String host;
    private String roadName;
    private int threads = Runtime.getRuntime().availableProcessors();
    private RetryHandler retryHandler = RetryHandler.retryOnce();
    private ObjectMapper objectMapper = new ObjectMapper();
    private TLSConfig.Factory tlsConfigFactory;

    /**
     * Optional. The user name credential to authenticate with Data Highway.
     *
     * @param username The username.
     * @return {@link Builder this}
     */
    public Builder username(String username) {
      this.username = username;
      return this;
    }

    /**
     * Optional. The password to authenticate with Data Highway.
     *
     * @param password The password.
     * @return {@link Builder this}
     */
    public Builder password(String password) {
      this.password = password;
      return this;
    }

    /**
     * Required. The Data Highway host.
     *
     * @param host The Data Highway host.
     * @return {@link Builder this}
     */
    public Builder host(@NonNull String host) {
      this.host = host;
      return this;
    }

    /**
     * Required. The road name.
     *
     * @param roadName The road name.
     * @return {@link Builder this}
     */
    public Builder roadName(@NonNull String roadName) {
      this.roadName = roadName;
      return this;
    }

    /**
     * Optional. Specifies number of parallel threads accessing the client.
     * Default - Same as number of processors
     *
     * @param threads number of parallel threads.
     * @return {@link Builder this}
     */
    public Builder threads(int threads) {
      this.threads = threads;
      return this;
    }

    /**
     * Optional. Specifies the retry strategy while retrying send on error.
     * Default is {@link RetryHandler#retryOnce} - retries once immediately.
     *
     * @param retryHandler Whether retries are enabled.
     * @return {@link Builder this}
     */
    public Builder retryBehaviour(@NonNull RetryHandler retryHandler) {
      this.retryHandler = retryHandler;
      return this;
    }

    /**
     * Optional. Allows custom {@link ObjectMapper}
     *
     * @param objectMapper The {@link ObjectMapper}
     * @return {@link Builder this}
     */
    public Builder objectMapper(@NonNull ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    /**
     * Optional. Allows custom {@link SSLContext} and {@link HostnameVerifier} if alternative TLS behaviour is required.
     *
     * @param tlsConfigFactory The {@link TLSConfig.Factory}
     * @return {@link Builder this}
     */
    public Builder tlsConfigFactory(TLSConfig.Factory tlsConfigFactory) {
      this.tlsConfigFactory = tlsConfigFactory;
      return this;
    }

    /**
     * Creates an {@link OnrampOptions} instance.
     *
     * @return {@link OnrampOptions}
     */
    public OnrampOptions build() {
      return new OnrampOptions(username, password, host, roadName, threads, retryHandler, objectMapper,
          tlsConfigFactory);
    }
  }
}
