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
package com.hotels.road.highwaypatrol;

import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.hotels.road.boot.DataHighwayApplication;
import com.hotels.road.client.AsyncRoadClient;
import com.hotels.road.client.OnrampOptions;
import com.hotels.road.client.partitioning.PartitioningRoadClientBuilder;
import com.hotels.road.offramp.client.OfframpClient;
import com.hotels.road.offramp.client.OfframpOptions;

@SpringBootApplication
public class HighwayPatrolApp {
  @Bean
  public AsyncRoadClient<TestMessage> onrampClient(
      @Value("${dataHighwayHost}") String dataHighwayHost,
      @Value("${username}") String username,
      @Value("${password}") String password,
      @Value("${roadName}") String roadName,
      @Value("${senderThreads}") int threads,
      @Value("${maxBatchSize}") int maxBatchSize,
      @Value("${bufferSize}") int bufferSize)
    throws URISyntaxException {
    OnrampOptions options = OnrampOptions
        .builder()
        .host(dataHighwayHost)
        .username(username)
        .password(password)
        .roadName(roadName)
        .threads(threads)
        .build();
    return new PartitioningRoadClientBuilder<TestMessage>(options)
        .withMaxBatchSize(maxBatchSize)
        .withBufferSize(bufferSize)
        .build();
  }

  @Bean
  OfframpClient<TestMessage> offrampClient(
      @Value("${dataHighwayHost}") String dataHighwayHost,
      @Value("${username}") String username,
      @Value("${password}") String password,
      @Value("${roadName}") String roadName,
      @Value("${streamName:highway_patrol}") String streamName) {
    return OfframpClient.create(OfframpOptions
        .builder(TestMessage.class)
        .username(username)
        .password(password)
        .host(dataHighwayHost)
        .roadName(roadName)
        .streamName(streamName)
        .build());
  }

  @Bean
  public ScheduledExecutorService contextWorkerService() {
    return Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("context-worker-%d").build());
  }

  @Bean
  public ApplicationRunner runner(Receiver receiver, Sender sender) {
    return args -> {
      receiver.start();
      sender.start();
    };
  }

  public static void main(String[] args) {
    DataHighwayApplication.run(HighwayPatrolApp.class, args);
  }
}
