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
package com.hotels.road.kafka.healthcheck;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.google.common.collect.ImmutableMap;

@RequiredArgsConstructor
@Component
public class KafkaConsumerFactory {

  private static final StringDeserializer deserializer = new StringDeserializer();

  @Value("${kafka.bootstrapServers}")
  private final String bootstrapServers;

  public Consumer<String, String> create(String groupId) {
    Map<String, Object> config = ImmutableMap
        .<String, Object> builder()
        .put("bootstrap.servers", bootstrapServers)
        .put("group.id", groupId)
        .put("session.timeout.ms", 4000)
        .put("request.timeout.ms", 5000)
        .build();

    return new KafkaConsumer<String, String>(config, deserializer, deserializer);
  }
}
