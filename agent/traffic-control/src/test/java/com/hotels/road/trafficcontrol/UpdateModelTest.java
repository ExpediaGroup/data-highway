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
package com.hotels.road.trafficcontrol;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import static com.hotels.road.tollbooth.client.api.PatchOperation.add;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.rest.model.RoadType;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;
import com.hotels.road.trafficcontrol.model.KafkaRoad;
import com.hotels.road.trafficcontrol.model.MessageStatus;

@RunWith(MockitoJUnitRunner.class)
public class UpdateModelTest {
  public Map<String, KafkaRoad> store;
  public @Mock PatchSetEmitter emitter;
  public @Mock KafkaAdminClient adminClient;

  public UpdateModel messageStatusUpdator;
  public KafkaRoad testRoad = new KafkaRoad("test_road",
                    "road.test_road", RoadType.NORMAL, null, null, false);

  @Before
  public void before() throws Exception {
    store = ImmutableMap.of("test_road", testRoad);
    messageStatusUpdator = new UpdateModel(store, emitter, adminClient);
  }

  @Test
  public void operations_are_passed_to_emitter() throws Exception {
    MessageStatus status = new MessageStatus(15134544543L, 100);
    given(adminClient.updateMessageStatus(testRoad)).willReturn(singletonList(add("/messageStatus", status)));
    messageStatusUpdator.updateMessageStatusInModel();
    then(emitter).should().emit(new PatchSet("test_road", singletonList(add("/messageStatus", status))));
  }

  @Test
  public void no_operations_means_no_PatchSet() throws Exception {
    given(adminClient.updateMessageStatus(testRoad)).willReturn(emptyList());
    messageStatusUpdator.updateMessageStatusInModel();
    then(emitter).shouldHaveZeroInteractions();
  }

}
