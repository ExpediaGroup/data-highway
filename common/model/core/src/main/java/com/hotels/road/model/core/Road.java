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
package com.hotels.road.model.core;

import java.util.HashMap;
import java.util.Map;

import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.RoadType;
import com.hotels.road.schema.chronology.SchemaCompatibility;

import lombok.Data;

@Data
public class Road {
  public static final String DEFAULT_COMPATIBILITY_MODE = SchemaCompatibility.CAN_READ_ALL.name();
  private String name;
  private RoadType type = RoadType.NORMAL;
  private String topicName;
  private String description;
  private String teamName;
  private String contactEmail;
  private boolean enabled;
  private long enabledTimeStamp;
  private String partitionPath;
  private Authorisation authorisation;
  private Map<String, String> metadata = new HashMap<>();
  private Map<Integer, SchemaVersion> schemas = new HashMap<>();
  private Map<String, Destination> destinations = new HashMap<>();
  private KafkaStatus status;
  private String compatibilityMode = DEFAULT_COMPATIBILITY_MODE;
  private MessageStatus messageStatus;
  private boolean deleted;
}
