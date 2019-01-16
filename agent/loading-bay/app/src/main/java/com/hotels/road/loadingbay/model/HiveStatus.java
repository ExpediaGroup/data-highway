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
package com.hotels.road.loadingbay.model;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import com.hotels.road.loadingbay.serde.OffsetDateTimeDeserializer;
import com.hotels.road.loadingbay.serde.OffsetDateTimeSerializer;

@JsonDeserialize(builder = HiveStatus.HiveStatusBuilder.class)
@lombok.Data
@lombok.Builder
public class HiveStatus {
  private final boolean hiveTableCreated;
  private final int hiveSchemaVersion;
  private final String message;
  private final OffsetDateTime lastRun;

  @JsonSerialize(using = OffsetDateTimeSerializer.class)
  public OffsetDateTime getLastRun() {
    return lastRun;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class HiveStatusBuilder {

    @JsonDeserialize(using = OffsetDateTimeDeserializer.class)
    public HiveStatusBuilder lastRun(OffsetDateTime lastRun) {
      this.lastRun = lastRun;
      return this;
    }
  }
}
