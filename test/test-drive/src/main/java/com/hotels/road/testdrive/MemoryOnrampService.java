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
package com.hotels.road.testdrive;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampService;

@Component
@RequiredArgsConstructor
class MemoryOnrampService implements OnrampService {
  private final Map<String, Road> store;
  private final Map<String, List<Record>> messages;

  @Override
  public Optional<Onramp> getOnramp(String name) {
    return Optional.of(name).map(store::get).map(road -> new MemoryOnramp(road, messages));
  }
}
