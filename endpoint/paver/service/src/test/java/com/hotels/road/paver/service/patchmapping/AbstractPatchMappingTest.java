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
package com.hotels.road.paver.service.patchmapping;

import java.util.HashMap;

import org.junit.Before;

import com.hotels.road.model.core.KafkaStatus;
import com.hotels.road.model.core.Road;

public abstract class AbstractPatchMappingTest {
  protected Road road;
  protected KafkaStatus status;

  @Before
  public void before() throws Exception {
    status = new KafkaStatus();
    status.setTopicCreated(false);

    road = new Road();
    road.setName("road1");
    road.setTopicName("road.road1");
    road.setDescription("description");
    road.setTeamName("teamName");
    road.setContactEmail("contactEmail");
    road.setMetadata(new HashMap<>());
    road.setStatus(status);
  }
}
