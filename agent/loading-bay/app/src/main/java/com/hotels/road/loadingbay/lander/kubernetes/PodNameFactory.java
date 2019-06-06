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
package com.hotels.road.loadingbay.lander.kubernetes;

import static com.google.common.base.Preconditions.checkState;

import static com.hotels.road.loadingbay.lander.TruckParkConstants.TRUCK_PARK;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.springframework.stereotype.Component;

import com.google.common.collect.Iterables;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import com.hotels.road.loadingbay.lander.LanderConfiguration;

@Component
class PodNameFactory {
  private static final int MAX_POD_NAME_LENGTH = 63;
  private static final int MAX_HASH_STRING_LENGTH = 4;

  private static final HashFunction hashFunction = Hashing.goodFastHash(16);

  String newName(LanderConfiguration config) {
    // Pod name needs to adhere this regex: "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$".
    // Pod name must be 63 characters or less
    String prefix = TRUCK_PARK + "-";
    int partition = Iterables.getFirst(config.getOffsets().keySet(), 0);
    String suffix = "-" + config.getAcquisitionInstant().toLowerCase(Locale.ROOT) + "-" + partition;

    String roadName = config.getRoadName().toLowerCase().replaceAll("_", "-");
    String hash = hashFunction
        .hashString(roadName, StandardCharsets.UTF_8)
        .toString()
        .substring(0, MAX_HASH_STRING_LENGTH)
        .toLowerCase(Locale.ROOT);

    int remainingChars = MAX_POD_NAME_LENGTH - prefix.length() - suffix.length();

    checkState(remainingChars > MAX_HASH_STRING_LENGTH,
        "between the process name and partition column value there is not enough room for the road name");

    if (remainingChars < roadName.length()) {
      roadName = roadName.substring(0, remainingChars - hash.length()) + hash;
    }

    return prefix + roadName + suffix;
  }
}
