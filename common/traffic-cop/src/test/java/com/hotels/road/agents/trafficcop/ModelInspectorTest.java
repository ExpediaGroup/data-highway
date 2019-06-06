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
package com.hotels.road.agents.trafficcop;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import static com.hotels.road.tollbooth.client.api.PatchOperation.add;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.agents.trafficcop.spi.ModelReader;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@RunWith(MockitoJUnitRunner.class)
public class ModelInspectorTest {
  public Map<String, Integer> store;

  public @Mock Agent<Integer> agent;
  public @Mock PatchSetEmitter emitter;

  public ModelReader<Integer> modelReader = JsonNode::asInt;

  public ModelInspector<Integer> inspector;

  @Before
  public void before() throws Exception {
    store = ImmutableMap.of("one", 1, "two", 2);
    inspector = new ModelInspector<>(store, agent, emitter);
  }

  @Test
  public void operations_are_passed_to_emitter() throws Exception {
    given(agent.inspectModel("one", 1)).willReturn(singletonList(add("/value", 1)));
    given(agent.inspectModel("two", 2)).willReturn(singletonList(add("/value", 2)));

    inspector.inspect();

    then(emitter).should().emit(new PatchSet("one", singletonList(add("/value", 1))));
    then(emitter).should().emit(new PatchSet("two", singletonList(add("/value", 2))));
  }

  @Test
  public void no_operations_means_no_PatchSet() throws Exception {
    given(agent.inspectModel("one", 1)).willReturn(emptyList());
    given(agent.inspectModel("two", 2)).willReturn(emptyList());

    inspector.inspect();

    then(emitter).shouldHaveZeroInteractions();
  }

  @Test
  public void inspectModel_throwing_doesnot_stop_execution() throws Exception {
    given(agent.inspectModel(anyString(), any(Integer.class))).willThrow(RuntimeException.class);

    inspector.inspect();

    then(agent).should(times(2)).inspectModel(anyString(), any(Integer.class));
  }
}
