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
package com.hotels.road.trafficcontrol.function;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.math.MathFlux;

@Component
@RequiredArgsConstructor
public class MessageCountPerTopicFunction {
  private final KafkaConsumer<?, ?> consumer;

  public long apply(Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> beginningOffsets;
    Map<TopicPartition, Long> endOffsets;
    synchronized (consumer) {
      beginningOffsets = consumer.beginningOffsets(partitions);
      endOffsets = consumer.endOffsets(partitions);
    }

    return MathFlux.sumLong(Flux.fromIterable(partitions)
      .map(p -> (endOffsets.get(p) - beginningOffsets.get(p)))).block();
  }
}
