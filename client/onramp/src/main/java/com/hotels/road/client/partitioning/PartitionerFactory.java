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
package com.hotels.road.client.partitioning;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.NonNull;

import com.hotels.road.client.OnrampOptions;
import com.hotels.road.client.RoadClient;
import com.hotels.road.client.simple.SimpleRoadClient;
import com.hotels.road.partition.MessageHashCodeFunctionSupplier;
import com.hotels.road.rest.model.StandardResponse;

public class PartitionerFactory<T> {

  public CloseableFunction<T, CompletableFuture<StandardResponse>> newInstance(
      OnrampOptions options,
      int bufferSize,
      int maxBatchSize,
      @NonNull EnqueueBehaviour enqueueBehaviour) {

    DefinitionSupplier definitionSupplier = new DefinitionSupplier(options);
    PartitionPathSupplier partitionPathSupplier = new PartitionPathSupplier(definitionSupplier);
    MessageHashCodeFunctionSupplier hashCodeFunctionSupplier = new MessageHashCodeFunctionSupplier(partitionPathSupplier);
    ScheduledSupplier<Function<JsonNode, Integer>> hasherSupplier = new ScheduledSupplier<>(hashCodeFunctionSupplier,
            5, MINUTES);
    RoadClient<JsonNode> client = new SimpleRoadClient<>(options);

    MessagePartitioner<JsonNode, CompletableFuture<StandardResponse>> partitioner = new MessagePartitioner<>(
            hasherSupplier, listFilledWith(options.getThreads(),
            () -> new MessageBatcher<>(bufferSize, maxBatchSize, enqueueBehaviour, client::sendMessages)));

    Function<T, CompletableFuture<StandardResponse>> function = partitioner.compose(options.getObjectMapper()::valueToTree);

    return new CloseableFunction<T, CompletableFuture<StandardResponse>>() {
      @Override
      public CompletableFuture<StandardResponse> apply(@NonNull T message) {
        return function.apply(message);
      }

      @Override
      public void close() throws Exception {
        partitioner.close();
        hasherSupplier.close();
        client.close();
      }
    };
  }

  private <I> List<I> listFilledWith(int size, Supplier<I> supplier) {
    List<I> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(supplier.get());
    }
    return list;
  }

}
