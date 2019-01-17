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
package com.hotels.road.offramp.client;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;

import static lombok.AccessLevel.PACKAGE;

import static com.hotels.road.offramp.model.DefaultOffset.LATEST;
import static com.hotels.road.rest.model.Sensitivity.PUBLIC;

import java.io.Serializable;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.rest.model.Sensitivity;
import com.hotels.road.tls.TLSConfig;

@EqualsAndHashCode(exclude = {"payloadTypeFactory"})
@RequiredArgsConstructor(access = PACKAGE)
public class OfframpOptions<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final @Getter String username;
  private final @Getter String password;
  private final @NonNull String host;
  private final @NonNull String roadName;
  private final @NonNull String streamName;
  private final @NonNull DefaultOffset defaultOffset;
  private final @NonNull Set<Sensitivity> grants;
  private final @NonNull Class<T> payloadClass;
  private final @NonNull PayloadTypeFactory payloadTypeFactory;
  private final PayloadDeserializer<T> payloadDeserialiser;
  private final @Getter(PACKAGE) boolean retry;
  private final @Getter(PACKAGE) int initialRequestAmount;
  private final @Getter(PACKAGE) int replenishingRequestAmount;
  private final @Getter(PACKAGE) long keepAliveSeconds;
  private final @Getter(PACKAGE) TLSConfig.Factory tlsConfigFactory;

  URI uri() {
    String grantsParam = grants.stream().map(Sensitivity::name).collect(joining(","));
    return URI.create(String.format("wss://%s/offramp/v2/roads/%s/streams/%s/messages?defaultOffset=%s&grants=%s", host,
        roadName, streamName, defaultOffset.name(), grantsParam));
  }

  ObjectMapper objectMapper() {
    return ObjectMapperFactory.create(payloadClass, payloadTypeFactory, payloadDeserialiser);
  }

  public static <T> Builder<T> builder(Class<T> payloadClass) {
    return new Builder<T>(payloadClass);
  }

  @RequiredArgsConstructor(access = PACKAGE)
  public static class Builder<T> {
    private String username;
    private String password;
    private String host;
    private String roadName;
    private String streamName;
    private DefaultOffset defaultOffset = LATEST;
    private Set<Sensitivity> grants = emptySet();
    private final Class<T> payloadClass;
    private PayloadTypeFactory payloadTypeFactory;
    private PayloadDeserializer<T> payloadDeserialiser;
    private boolean retry = true;
    private int initialRequestAmount = 4000;
    private int replenishingRequestAmount = 1000;
    private long keepAliveSeconds = 30;
    private TLSConfig.Factory tlsConfigFactory;

    /**
     * Required. The user name credential to authenticate with Data Highway.
     *
     * @param username The username.
     * @return {@link Builder this}
     */
    public Builder<T> username(@NonNull String username) {
      this.username = username;
      return this;
    }

    /**
     * Required. The password to authenticate with Data Highway.
     *
     * @param password The password.
     * @return {@link Builder this}
     */
    public Builder<T> password(@NonNull String password) {
      this.password = password;
      return this;
    }

    /**
     * Required. The Data Highway host.
     *
     * @param host The Data Highway host.
     * @return {@link Builder this}
     */
    public Builder<T> host(@NonNull String host) {
      this.host = host;
      return this;
    }

    /**
     * Required. The road name.
     *
     * @param roadName The road name.
     * @return {@link Builder this}
     */
    public Builder<T> roadName(@NonNull String roadName) {
      this.roadName = roadName;
      return this;
    }

    /**
     * Required. The stream name.
     *
     * @param streamName The stream name.
     * @return {@link Builder this}
     */
    public Builder<T> streamName(@NonNull String streamName) {
      this.streamName = streamName;
      return this;
    }

    /**
     * Optional. The {@link DefaultOffset} to start streaming at on each partition if no commit exists. Defaults to
     * {@link DefaultOffset#LATEST LATEST} if not provided.
     *
     * @param defaultOffset The {@link DefaultOffset}.
     * @return {@link Builder this}
     */
    public Builder<T> defaultOffset(@NonNull DefaultOffset defaultOffset) {
      this.defaultOffset = defaultOffset;
      return this;
    }

    /**
     * Optional. Determines which data sensitivity types should be consumed unobfuscated. Public data is always served
     * unobfuscated.
     *
     * @param grants The {@link Set} of {@link Sensitivity} grants.
     * @return {@link Builder this}
     */
    public Builder<T> grants(@NonNull Set<Sensitivity> grants) {
      if (grants.contains(PUBLIC)) {
        throw new IllegalArgumentException("PUBLIC is not a required grant");
      }
      this.grants = new HashSet<>(grants);
      return this;
    }

    /**
     * Optional. Defaults to the equivalent of {@code tf -> tf.constructType(payloadClass)}. Will only be needed if the
     * payload type itself is a generic type.
     *
     * @param payloadTypeFactory The {@link PayloadTypeFactory}.
     * @return {@link Builder this}
     */
    public Builder<T> payloadTypeFactory(@NonNull PayloadTypeFactory payloadTypeFactory) {
      this.payloadTypeFactory = payloadTypeFactory;
      return this;
    }

    /**
     * Optional. May be needed if any specific code is required for deserialisation to the payload type.
     *
     * @param payloadDeserialiser A custom {@link JsonDeserializer}.
     * @return {@link Builder this}
     */
    public Builder<T> payloadDeserialiser(@NonNull PayloadDeserializer<T> payloadDeserialiser) {
      this.payloadDeserialiser = payloadDeserialiser;
      return this;
    }

    /**
     * Optional. Specifies whether the client should reacquire a connection to offramp and retry when errors occur.
     * Default is true - infinite retries with a backoff of 1 second.
     *
     * @param retry Whether retries are enabled.
     * @return {@link Builder this}
     */
    public Builder<T> retry(boolean retry) {
      this.retry = retry;
      return this;
    }

    /**
     * Optional. Parameters for configuring poll mechanism.
     *
     * @param initialRequestAmount Defines how many messages will be requested on the first call to Offramp Server.
     * @param replenishingRequestAmount Defines how many messages will be requested on subsequent requests. The
     *          subsequent requests will only happen after that many messages are consumed downstream.
     * @return {@link Builder this}
     */
    public Builder<T> requestBuffer(int initialRequestAmount, int replenishingRequestAmount) {
      this.initialRequestAmount = initialRequestAmount;
      this.replenishingRequestAmount = replenishingRequestAmount;
      return this;
    }

    /**
     * Optional. Specifies how often keep-alive signals should be sent. A value of less than or equal to 0 will disable
     * keep-alive signals. The default is 30. Enabling keep-alive signals would suit consumers of roads with sparse
     * data, allowing them to stay connected during periods of no data.
     *
     * @param keepAliveSeconds The number
     * @return {@link Builder this}
     */
    public Builder<T> keepAliveSeconds(long keepAliveSeconds) {
      this.keepAliveSeconds = keepAliveSeconds;
      return this;
    }

    /**
     * Optional. Allows custom {@link SSLContext} and {@link HostnameVerifier} if alternative TLS behaviour is required.
     *
     * @param tlsConfigFactory The {@link TLSConfig.Factory}
     * @return {@link Builder this}
     */
    public Builder<T> tlsConfigFactory(TLSConfig.Factory tlsConfigFactory) {
      this.tlsConfigFactory = tlsConfigFactory;
      return this;
    }

    /**
     * Creates an {@link OfframpOptions} instance.
     *
     * @return {@link OfframpOptions}
     */
    public OfframpOptions<T> build() {
      if ((username == null && password != null) || (username != null && password == null)) {
        throw new IllegalStateException("Both username and password must be set or neither");
      }

      if (payloadTypeFactory == null) {
        payloadTypeFactory = PayloadTypeFactory.fromClass(payloadClass);
      }
      return new OfframpOptions<>(username, password, host, roadName, streamName, defaultOffset, grants, payloadClass,
          payloadTypeFactory, payloadDeserialiser, retry, initialRequestAmount, replenishingRequestAmount,
          keepAliveSeconds, tlsConfigFactory);
    }
  }
}
