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
package com.hotels.road.rest.model.validator;

import java.util.regex.Pattern;

public class RoadNameValidator implements ModelValidator<String> {
  private static final Pattern ROAD_NAME_PATTERN = Pattern.compile("^[a-z][a-z0-9_]*$");
  private static final String ERROR_MESSAGE = "Road name must start with a lower case letter and be followed by zero or more lower case letters, numbers or underscore characters.";
  private static final RoadNameValidator VALIDATOR = new RoadNameValidator();

  @Override
  public String validate(String roadName) throws InvalidRoadNameException {
    if (roadName == null || !ROAD_NAME_PATTERN.matcher(roadName).matches()) {
      throw new InvalidRoadNameException(ERROR_MESSAGE);
    }
    return roadName;
  }

  public static String validateRoadName(String roadName) {
    return VALIDATOR.validate(roadName);
  }
}
