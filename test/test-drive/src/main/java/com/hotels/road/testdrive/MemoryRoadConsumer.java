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
package com.hotels.road.testdrive;

import static java.util.Collections.singleton;

import static com.hotels.road.offramp.model.DefaultOffset.LATEST;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.offramp.spi.RoadConsumer;

@AllArgsConstructor
class MemoryRoadConsumer implements RoadConsumer {
  private final List<Record> messages;
  private final AtomicInteger commit;
  private int offset;

  @Override
  public void init(long initialRequest, RebalanceListener rebalanceListener) {
    rebalanceListener.onRebalance(singleton(0));
  }

  @Override
  public Iterable<Record> poll() {
    if (offset < messages.size()) {
      Record record = messages.get(offset);
      offset++;
      return Collections.singletonList(record);
    }
    return Collections.emptyList();
  }

  @Override
  public boolean commit(Map<Integer, Long> offsets) {
    Long o = offsets.get(0);
    if (o != null) {
      commit.set(o.intValue());
    }
    return true;
  }

  @Override
  public void close() {}

  @Component
  @RequiredArgsConstructor
  static class Factory implements RoadConsumer.Factory {
    private final Map<String, List<Record>> messages;
    private final Map<StreamKey, AtomicInteger> commits;

    @Override
    public RoadConsumer create(String roadName, String streamName, DefaultOffset defaultOffset)
      throws UnknownRoadException {
      List<Record> roadMessages = messages.computeIfAbsent(roadName, n -> new ArrayList<>());
      if (roadMessages == null) {
        throw new UnknownRoadException("Unknown road: " + roadName);
      }

      StreamKey streamKey = new StreamKey(roadName, streamName);

      AtomicInteger commit = commits.computeIfAbsent(streamKey, k -> new AtomicInteger(-1));
      int offset = 0;
      if (commit.get() == -1) {
        if (defaultOffset == LATEST) {
          offset = roadMessages.size();
        }
      } else {
        offset = commit.get();
      }

      return new MemoryRoadConsumer(roadMessages, commit, offset);
    }
  }

  @Data
  static class StreamKey {
    private final String roadName;
    private final String streamName;
  }
}
