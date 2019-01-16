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
package com.hotels.road.truck.park.s3;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

import java.time.Clock;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class KeySupplier implements Supplier<String> {
  private final String prefix;
  private final Clock clock;
  private final Supplier<String> randomSuffix = () -> randomAlphabetic(8);

  @Autowired
  KeySupplier(@Value("${s3.keyPrefix}") String prefix, Clock clock) {
    this.prefix = prefix;
    this.clock = clock;
  }

  @Override
  public String get() {
    return String.format("%s/%d_%s", prefix, clock.millis(), randomSuffix.get());
  }

}
