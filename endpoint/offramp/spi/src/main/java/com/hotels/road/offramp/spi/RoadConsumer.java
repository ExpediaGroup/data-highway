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
package com.hotels.road.offramp.spi;

import java.util.Map;
import java.util.Set;

import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.model.DefaultOffset;

public interface RoadConsumer extends AutoCloseable {
  void init(long initialRequest, RebalanceListener rebalanceListener);

  Iterable<Record> poll();

  boolean commit(Map<Integer, Long> offsets);

  public interface Factory {
    RoadConsumer create(String roadName, String streamName, DefaultOffset defaultOffset) throws UnknownRoadException;
  }

  public interface RebalanceListener {
    void onRebalance(Set<Integer> assignment);
  }
}
