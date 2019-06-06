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
package com.hotels.road.rest.model;

import java.util.Map;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
@EqualsAndHashCode(callSuper = true)
public class RoadModel extends BasicRoadModel {
  @ApiModelProperty(name = "roadIntact", value = "Indicates if the road is ready to accept messages.", required = false)
  private final boolean roadIntact;
  @ApiModelProperty(name = "compatibilityMode", value = "Specifies which compatibility mode is used on the road", required = false)
  private final String compatibilityMode;
  @ApiModelProperty(name = "agentMessages", value = "A map of agents' status or error messages affecting the road.", required = false)
  private final Map<String, String> agentMessages;

  @JsonCreator
  public RoadModel(
      @JsonProperty("name") String name,
      @JsonProperty("type") RoadType type,
      @JsonProperty("description") String description,
      @JsonProperty("teamName") String teamName,
      @JsonProperty("contactEmail") String contactEmail,
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("partitionPath") String partitionPath,
      @JsonProperty("authorisation") Authorisation authorisation,
      @JsonProperty("metadata") Map<String, String> metadata,
      @JsonProperty("roadIntact") boolean roadIntact,
      @JsonProperty("compatibilityMode") String compatibilityMode,
      @JsonProperty("agentMessages") Map<String, String> agentMessages) {
    super(name, description, teamName, contactEmail, enabled, partitionPath, authorisation, metadata);
    this.roadIntact = roadIntact;
    this.compatibilityMode = compatibilityMode;
    this.agentMessages = agentMessages;
  }
}
