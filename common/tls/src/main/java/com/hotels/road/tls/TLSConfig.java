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
package com.hotels.road.tls;

import java.io.Serializable;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
public class TLSConfig {
  @NonNull
  SSLContext sslContext;
  @NonNull
  HostnameVerifier hostnameVerifier;

  /**
   * Disables certificate checking and hostname verification. This is intended for testing only.
   *
   * @return {@link TLSConfig}
   */
  public static TLSConfig trustAll() {
    log.warn(
        "TLSConfig.trustAll() disables certificate checking and hostname verification. This is intended for testing only.");
    return new TLSConfig(trustAllSSLContext(), trustAllHostnameVerifier());
  }

  /**
   * Disables certificate checking and hostname verification. This is intended for testing only.
   *
   * @return {@link TLSConfig}
   */
  public static TLSConfig.Factory trustAllFactory() {
    return new TrustAllFactory();
  }

  public static HostnameVerifier trustAllHostnameVerifier() {
    return (hostname, session) -> true;
  }

  public static SSLContext trustAllSSLContext() {
    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustAllTrustManager(), new SecureRandom());
      return sslContext;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException(e);
    }
  }

  private static TrustManager[] trustAllTrustManager() {
    return new TrustManager[] { new X509TrustManager() {
      @Override
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      @Override
      public void checkClientTrusted(X509Certificate[] certs, String authType) {}

      @Override
      public void checkServerTrusted(X509Certificate[] certs, String authType) {}
    } };
  }

  public interface Factory extends Serializable {
    TLSConfig create();
  }

  public static class TrustAllFactory implements Factory {
    private static final long serialVersionUID = 1L;

    @Override
    public TLSConfig create() {
      return trustAll();
    }
  }
}
