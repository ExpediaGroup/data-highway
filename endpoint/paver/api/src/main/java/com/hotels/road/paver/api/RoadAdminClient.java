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
package com.hotels.road.paver.api;

import java.util.Optional;
import java.util.SortedSet;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.Road;
import com.hotels.road.tollbooth.client.api.PatchSet;

public interface RoadAdminClient {

  /**
   * @return All Road names or an empty collection if there are none.
   * @throws ServiceException Any other error.
   */
  SortedSet<String> listRoads() throws ServiceException;

  /**
   * @param name The road name.
   * @return The Road's metadata.
   * @throws IllegalArgumentException If the name is blank.
   * @throws ServiceException Any other error.
   */
  Optional<Road> getRoad(String name) throws IllegalArgumentException, ServiceException;

  /**
   * @param road The Road's metadata.
   * @throws AlreadyExistsException If the road already exists.
   * @throws IllegalArgumentException If any information is missing.
   * @throws ServiceException Any other error.
   */
  void createRoad(Road road) throws AlreadyExistsException, IllegalArgumentException, ServiceException;

  /**
   * @param patch The patch to apply.
   * @throws UnknownRoadException If the road does not exist.
   * @throws IllegalArgumentException If any information is missing.
   * @throws ServiceException Any other error.
   */
  void updateRoad(PatchSet patch) throws UnknownRoadException, IllegalArgumentException, ServiceException;

}
