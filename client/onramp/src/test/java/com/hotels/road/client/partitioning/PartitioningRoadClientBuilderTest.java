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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.client.AsyncRoadClient;
import com.hotels.road.client.OnrampOptions;
import com.hotels.road.rest.model.StandardResponse;

@RunWith(MockitoJUnitRunner.class)
public class PartitioningRoadClientBuilderTest {

  @Mock
  private PartitionerFactory<String> partitionerFactory;
  @Mock
  ObjectMapper mapper;
  @Mock
  private CloseableFunction<String, CompletableFuture<StandardResponse>> function;

  @Test
  public void test() throws Exception {
    String roadName = "test_road";

    when(partitionerFactory.newInstance(any(OnrampOptions.class), anyInt(), anyInt(), any(EnqueueBehaviour.class)))
        .thenReturn(function);

    AsyncRoadClient<String> client = new PartitioningRoadClientBuilder<>(partitionerFactory, "host", "user", "pass",
            roadName).withThreads(1).withBufferSize(2).withMaxBatchSize(3).withObjectMapper(mapper).build();

    client.sendMessage("foo");
    verify(function).apply("foo");

    client.close();
    verify(function).close();
  }
}
