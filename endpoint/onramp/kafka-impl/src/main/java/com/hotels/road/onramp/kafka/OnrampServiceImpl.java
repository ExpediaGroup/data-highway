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
package com.hotels.road.onramp.kafka;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.road.model.core.Road;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampService;

@Component
public class OnrampServiceImpl implements OnrampService {
  private final OnrampMetrics metrics;
  private final Map<String, Road> roads;
  private final Producer<byte[], byte[]> kafkaProducer;

  @Autowired
  public OnrampServiceImpl(
      OnrampMetrics metrics,
      @Value("#{store}") Map<String, Road> roads,
      Producer<byte[], byte[]> kafkaProducer) {
    this.metrics = metrics;
    this.roads = roads;
    this.kafkaProducer = kafkaProducer;
  }

  @Override
  public Optional<Onramp> getOnramp(String name) {
    return Optional.ofNullable(roads.get(name)).map(road -> new OnrampImpl(metrics, kafkaProducer, road));
  }
}
