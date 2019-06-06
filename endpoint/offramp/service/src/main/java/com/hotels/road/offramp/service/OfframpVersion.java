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
package com.hotels.road.offramp.service;

import static lombok.AccessLevel.PRIVATE;

import java.util.Objects;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PRIVATE)
public enum OfframpVersion {
  OFFRAMP_2("2"),
  UNKNOWN(null);

  private final String versionString;

  public static OfframpVersion fromString(String versionString) {
    for (OfframpVersion version : values()) {
      if (Objects.equals(version.versionString, versionString)) {
        return version;
      }
    }
    return UNKNOWN;
  }
}
