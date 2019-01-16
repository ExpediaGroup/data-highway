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
package com.hotels.road.rest.model;

import java.time.Duration;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class HiveDestinationModel {
  public static final String MINIMUM_DURATION_STRING = "PT5M";
  public static final String MAXIMUM_DURATION_STRING = "P1D";
  // Using Duration::parse instead of Duration::ofMinutes because LANDING_INTERVAL_DESCRIPTION needs to be a compile
  // time string so that it can be used in a annotation attribute.
  public static final Duration MINIMUM_DURATION = Duration.parse(MINIMUM_DURATION_STRING);
  public static final Duration MAXIMUM_DURATION = Duration.parse(MAXIMUM_DURATION_STRING);
  private static final String LANDING_INTERVAL_DESCRIPTION = "Specifies how often data is landed to Hive, defaults to \"PT1H\". The format is an ISO 8601 Duration, see https://en.wikipedia.org/wiki/ISO_8601#Durations. The value must fall between "
      + MINIMUM_DURATION_STRING
      + " and "
      + MAXIMUM_DURATION_STRING;

  @ApiModelProperty(name = "enabled", value = "Specifies if the destination is enabled.")
  private boolean enabled;

  @ApiModelProperty(name = "landingInterval", value = LANDING_INTERVAL_DESCRIPTION, example = "\"PT1H\"")
  private String landingInterval;

}
