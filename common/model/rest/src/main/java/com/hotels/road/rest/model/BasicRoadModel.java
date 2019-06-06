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

import java.util.HashMap;
import java.util.Map;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.hotels.road.rest.model.validator.RoadNameValidator;

@Data
public class BasicRoadModel {
  @ApiModelProperty(name = "name", value = "Road name", required = true)
  protected final String name;
  @ApiModelProperty(name = "description", value = "A concise description of type and source of data available on the road.", required = true)
  protected final String description;
  @ApiModelProperty(name = "teamName", value = "Team that pushes data onto this road.", required = true)
  protected final String teamName;
  @ApiModelProperty(name = "contactEmail", value = "Team's contact email.", required = true)
  protected final String contactEmail;
  @ApiModelProperty(name = "enabled", value = "Indicates if the road is enabled.", required = false)
  private final boolean enabled;
  @ApiModelProperty(name = "partitionPath", value = "The path within a JSON message for partitioning data on the road.", required = false)
  protected final String partitionPath;
  @ApiModelProperty(name = "authorisation", value = "Specifies application authorisation options.", required = true)
  protected final Authorisation authorisation;
  @ApiModelProperty(name = "metadata", value = "A map where additional information about the road that does not fit into any other fields can be stored.", required = false)
  protected final Map<String, String> metadata;

  @JsonCreator
  public BasicRoadModel(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty("description") String description,
      @JsonProperty("teamName") String teamName,
      @JsonProperty("contactEmail") String contactEmail,
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("partitionPath") String partitionPath,
      @JsonProperty("authorisation") Authorisation authorisation,
      @JsonProperty("metadata") Map<String, String> metadata) {
    this.name = RoadNameValidator.validateRoadName(name);
    this.description = description;
    this.teamName = teamName;
    this.contactEmail = contactEmail;
    this.enabled = enabled;
    this.partitionPath = partitionPath;
    this.authorisation = authorisation;
    this.metadata = (metadata == null) ? null : new HashMap<>(metadata);
  }
}
