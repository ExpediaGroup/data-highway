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
package com.hotels.road.client.partitioning;

import java.util.concurrent.CompletableFuture;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.client.AsyncRoadClient;
import com.hotels.road.client.OnrampOptions;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.tls.TLSConfig;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class PartitioningRoadClientBuilder<T> {
  private final PartitionerFactory<T> partitionerFactory;
  private final OnrampOptions options;
  private final EnqueueBehaviour enqueueBehaviour;
  private int bufferSize = 1000;
  private int maxBatchSize = 100;

  @Deprecated
  /** @deprecated Use {@link #PartitioningRoadClientBuilder(PartitionerFactory, OnrampOptions)} */
  public PartitioningRoadClientBuilder(String host, String username, String password, String roadName) {
    this(toOptions(host, username, password, roadName));
  }

  @Deprecated
  /** @deprecated Use {@link #PartitioningRoadClientBuilder(PartitionerFactory, OnrampOptions)} */
  public PartitioningRoadClientBuilder(
      PartitionerFactory<T> partitionerFactory,
      String host,
      String username,
      String password,
      String roadName) {
    this(partitionerFactory, toOptions(host, username, password, roadName));
  }

  public PartitioningRoadClientBuilder(PartitionerFactory<T> partitionerFactory, OnrampOptions options) {
    this(partitionerFactory, options, EnqueueBehaviour.COMPLETE_EXCEPTIONALLY);
  }

  public PartitioningRoadClientBuilder(OnrampOptions options) {
    this(new PartitionerFactory<>(), options);
  }

  public PartitioningRoadClientBuilder<T> withBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
    return this;
  }

  public PartitioningRoadClientBuilder<T> withMaxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
    return this;
  }

  public PartitioningRoadClientBuilder<T> withOptions(OnrampOptions options) {
    return new PartitioningRoadClientBuilder<>(partitionerFactory, options, enqueueBehaviour)
        .withBufferSize(bufferSize)
        .withMaxBatchSize(maxBatchSize);
  }

  public PartitioningRoadClientBuilder<T> withEnqueueBehaviour(EnqueueBehaviour enqueueBehaviour) {
    return new PartitioningRoadClientBuilder<>(partitionerFactory, options, enqueueBehaviour)
        .withBufferSize(bufferSize)
        .withMaxBatchSize(maxBatchSize);
  }

  @Deprecated
  /** @deprecated Use {@link #withOptions(OnrampOptions)} */
  public PartitioningRoadClientBuilder<T> withThreads(int threads) {
    return withOptions(options.withThreads(threads));
  }

  @Deprecated
  /** @deprecated Use {@link #withOptions(OnrampOptions)} */
  public PartitioningRoadClientBuilder<T> withObjectMapper(ObjectMapper mapper) {
    return withOptions(options.withObjectMapper(mapper));
  }

  @Deprecated
  /** @deprecated Use {@link #withOptions(OnrampOptions)} */
  public PartitioningRoadClientBuilder<T> withTLSConfig(TLSConfig tlsConfig) {
    return withOptions(options.withTlsConfigFactory(() -> tlsConfig));
  }

  public PartitioningRoadClientBuilder<T> blockOnFullQueue() {
    return withEnqueueBehaviour(EnqueueBehaviour.BLOCK_AND_WAIT);
  }

  public PartitioningRoadClientBuilder<T> completeExceptionallyOnFullQueue() {
    return withEnqueueBehaviour(EnqueueBehaviour.COMPLETE_EXCEPTIONALLY);
  }

  public AsyncRoadClient<T> build() {
    CloseableFunction<T, CompletableFuture<StandardResponse>> function = partitionerFactory.newInstance(options,
        bufferSize, maxBatchSize, enqueueBehaviour);

    return new AsyncRoadClient<T>() {
      @Override
      public CompletableFuture<StandardResponse> sendMessage(T message) {
        return function.apply(message);
      }

      @Override
      public void close() throws Exception {
        function.close();
      }
    };
  }

  private static OnrampOptions toOptions(String host, String username, String password, String roadName) {
    return OnrampOptions.builder().host(host).username(username).password(password).roadName(roadName).build();
  }
}
