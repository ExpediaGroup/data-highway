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
package com.hotels.road.agents.trafficcop;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.kafkastore.StoreUpdateObserver;

@RunWith(MockitoJUnitRunner.class)
public class MuteableStoreUpdateObserverTest {
  @Mock
  public StoreUpdateObserver<String, String> mockObserver;

  public MuteableStoreUpdateObserver<String, String> underTest;

  @Before
  public void before() throws Exception {
    underTest = new MuteableStoreUpdateObserver<>(mockObserver);
  }

  @Test
  public void handleNew_events_dropped_when_muted() throws Exception {
    underTest.mute();
    underTest.handleNew("key", "value");
    verifyZeroInteractions(mockObserver);
  }

  @Test
  public void handleUpdate_events_dropped_when_muted() throws Exception {
    underTest.mute();
    underTest.handleUpdate("key", "oldValue", "newValue");
    verifyZeroInteractions(mockObserver);
  }

  @Test
  public void handleRemove_events_dropped_when_muted() throws Exception {
    underTest.mute();
    underTest.handleRemove("key", "oldValue");
    verifyZeroInteractions(mockObserver);
  }

  @Test
  public void handleNew_events_pass_when_unmuted() throws Exception {
    underTest.unmute();
    underTest.handleNew("key", "value");
    verify(mockObserver).handleNew("key", "value");
  }

  @Test
  public void handleUpdate_events_pass_when_unmuted() throws Exception {
    underTest.unmute();
    underTest.handleUpdate("key", "oldValue", "newValue");
    verify(mockObserver).handleUpdate("key", "oldValue", "newValue");
  }

  @Test
  public void handleRemove_events_pass_when_unmuted() throws Exception {
    underTest.unmute();
    underTest.handleRemove("key", "oldValue");
    verify(mockObserver).handleRemove("key", "oldValue");
  }
}
