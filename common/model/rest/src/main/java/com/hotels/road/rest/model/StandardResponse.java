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

import static java.time.Clock.systemUTC;

import java.time.Clock;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

@Data
public class StandardResponse {
  @ApiModelProperty(name = "timestamp", value = "Timestamp in milliseconds indicating when the response was sent.", required = false)
  private final long timestamp;
  @ApiModelProperty(name = "success", value = "Indicates if the response was successfull or not.", required = false)
  private final boolean success;
  @ApiModelProperty(name = "message", value = "Contains message that was sent with the response.", required = false)
  private final String message;

  /*
   * @JsonCreator and @JsonProperty added to support older versions of Jackson. Specifically added to support
   * road-offramp-spark OfframpReceiver implementation. Jackson 2.6.5 contained in the current Spark 2.1.1 release can't
   * correctly deserialize JSON objects without these annotations.
   */
  @JsonCreator
  public StandardResponse(
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("success") boolean success,
      @JsonProperty("message") String message) {
    this.timestamp = timestamp;
    this.success = success;
    this.message = message;
  }

  public static StandardResponse successResponse(String message) {
    return new StandardResponse(systemUTC(), true, message);
  }

  public static StandardResponse failureResponse(String message) {
    return new StandardResponse(systemUTC(), false, message);
  }

  @VisibleForTesting
  StandardResponse(Clock clock, boolean success, String message) {
    this(clock.millis(), success, message);
  }
}
