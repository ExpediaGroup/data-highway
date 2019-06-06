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
package com.hotels.road.loadingbay.model;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = Hive.HiveBuilder.class)
@lombok.Data
@lombok.Builder
public class Hive {
  public static final String DEFAULT_LANDING_INTERVAL = Duration.ofHours(1).toString();
  private final String hivePartitionColumnName;
  private final String hivePartitionerFormatterPattern;
  private final String maxUncompressedFileSize;
  private final int maxIntervalBetweenUploads;
  private final boolean enabled;
  @lombok.Builder.Default
  private String landingInterval = DEFAULT_LANDING_INTERVAL;
  private final HiveStatus status;

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class HiveBuilder {}
}
