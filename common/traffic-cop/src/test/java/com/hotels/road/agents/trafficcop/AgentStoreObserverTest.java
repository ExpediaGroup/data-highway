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

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@RunWith(MockitoJUnitRunner.class)
public class AgentStoreObserverTest {
  private static final String JSON_STRING_1 = "{\"foo\":\"bar\"}";
  private static final String JSON_STRING_2 = "{\"foo\":\"baz\"}";
  private static final List<PatchOperation> OPERATIONS = singletonList(PatchOperation.add("/path", "boom"));

  public @Captor ArgumentCaptor<PatchSet> patchSet;

  public @Mock Agent<String> agent;
  public @Mock PatchSetEmitter emitter;

  private AgentStoreObserver<String> observer;

  @Before
  public void before() throws Exception {
    observer = new AgentStoreObserver<>(agent, emitter);
  }

  @Test
  public void handleNewTest() throws Exception {
    given(agent.newModel("key", JSON_STRING_1)).willReturn(OPERATIONS);

    observer.handleNew("key", JSON_STRING_1);

    verify(agent).newModel("key", JSON_STRING_1);
    verify(emitter).emit(patchSet.capture());
    assertThat(patchSet.getValue().getOperations(), is(OPERATIONS));
  }

  @Test
  public void handleUpdateTest() throws Exception {
    given(agent.updatedModel("key", JSON_STRING_1, JSON_STRING_2)).willReturn(OPERATIONS);

    observer.handleUpdate("key", JSON_STRING_1, JSON_STRING_2);

    verify(emitter).emit(patchSet.capture());
    assertThat(patchSet.getValue().getOperations(), is(OPERATIONS));
  }

  @Test
  public void handleUpdate_passes_when_models_equal() throws Exception {
    observer.handleUpdate("key", JSON_STRING_1, JSON_STRING_1);

    then(agent).shouldHaveZeroInteractions();
    then(emitter).shouldHaveZeroInteractions();
  }

  @Test
  public void handleRemoveTest() throws Exception {
    observer.handleRemove("key", JSON_STRING_1);

    verify(agent).deletedModel("key", JSON_STRING_1);
  }

  @Test
  public void dontSendPatchOnNewEmptyOps() throws Exception {
    given(agent.newModel("key", JSON_STRING_1)).willReturn(Collections.emptyList());

    observer.handleNew("key", JSON_STRING_1);

    then(emitter).shouldHaveZeroInteractions();
  }

  @Test
  public void dontSendPatchOnUpdateEmptyOps() throws Exception {
    given(agent.updatedModel("key", JSON_STRING_1, JSON_STRING_2)).willReturn(Collections.emptyList());

    observer.handleUpdate("key", JSON_STRING_1, JSON_STRING_2);

    then(emitter).shouldHaveZeroInteractions();
  }
}
