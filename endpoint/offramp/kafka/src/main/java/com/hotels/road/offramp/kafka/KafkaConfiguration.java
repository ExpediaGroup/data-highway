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
package com.hotels.road.offramp.kafka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.SchemaProvider;
import com.hotels.road.offramp.spi.RoadConsumer;

@Configuration
public class KafkaConfiguration {
  @Bean
  RoadConsumer.Factory roadConsumerFactory(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("#{store}") Map<String, Road> store,
      @Value("${pollTimeoutMillis:0}") long pollTimeoutMillis,
      @Value("${minMaxPollRecords:10}") int minMaxPollRecords,
      @Value("${maxMaxPollRecords:5000}") int maxMaxPollRecords,
      SchemaProvider schemaProvider) {
    return new KafkaRoadConsumer.Factory(bootstrapServers, store, pollTimeoutMillis, minMaxPollRecords,
        maxMaxPollRecords, schemaProvider);
  }
}
