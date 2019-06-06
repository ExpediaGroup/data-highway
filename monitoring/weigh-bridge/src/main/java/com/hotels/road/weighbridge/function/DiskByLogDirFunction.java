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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import lombok.Data;
import reactor.core.publisher.Flux;

import com.hotels.road.weighbridge.function.ReplicaByPartitionFunction.Replica;

@Component
public class DiskByLogDirFunction {
  public Map<String, Disk> apply(Map<TopicPartition, Replica> logDirsByPartition) {
    return Flux
        .fromIterable(logDirsByPartition.values())
        .map(Replica::getLogDir)
        .collect(toSet())
        .flatMapIterable(identity())
        .map(File::new)
        .collectMap(d -> d.getAbsolutePath(), d -> new Disk(d.getFreeSpace(), d.getTotalSpace()))
        .block();
  }

  @Data
  public static class Disk {
    private final long free;
    private final long total;
  }
}
