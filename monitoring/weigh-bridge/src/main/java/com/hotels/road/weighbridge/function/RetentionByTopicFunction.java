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
package com.hotels.road.weighbridge.function;

import static java.lang.Long.parseLong;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@Component
@RequiredArgsConstructor
public class RetentionByTopicFunction {
  private final AdminClient client;

  public Map<String, Duration> apply(Collection<String> topics) {
    return Flux
        .fromIterable(topics)
        .map(name -> new ConfigResource(TOPIC, name))
        .collectList()
        .map(crs -> client.describeConfigs(crs).all())
        .map(KafkaFutures::join)
        .flatMapIterable(Map::entrySet)
        .collectMap(e -> e.getKey().name(), this::retention)
        .block();
  }

  private Duration retention(Entry<ConfigResource, Config> e) {
    return Duration.ofMillis(parseLong(e.getValue().get("retention.ms").value()));
  }
}
