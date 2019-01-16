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
package com.hotels.road.weighbridge.function;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction.Replica;

@Component
public class SizeByPartitionFunction {
  public Map<TopicPartition, Long> apply(Map<TopicPartition, Replica> partitionsAndLogDir) {
    return Flux
        .fromIterable(partitionsAndLogDir.entrySet())
        .collectMap(Entry::getKey,
            entry -> Mono
                .just(entry)
                .map(e -> new File(e.getValue().getLogDir(), e.getKey().toString()))
                .flatMap(f -> Mono.justOrEmpty(f.listFiles()))
                .flatMapIterable(Arrays::asList)
                .map(File::length)
                .reduce(0L, Long::sum)
                .blockOptional()
                .orElse(0L))
        .block();
  }
}
