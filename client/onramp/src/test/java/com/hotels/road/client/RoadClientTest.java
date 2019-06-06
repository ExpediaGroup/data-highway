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
package com.hotels.road.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.road.rest.model.StandardResponse;

@RunWith(MockitoJUnitRunner.class)
public class RoadClientTest {

  private final StandardResponse response1 = new StandardResponse(1, true, null);
  private final StandardResponse response2 = new StandardResponse(2, true, null);
  private final StandardResponse response3 = new StandardResponse(3, true, null);

  @Test
  public void defaultMethod() {
    @SuppressWarnings("unchecked")
    RoadClient<Integer> underTest = mock(RoadClient.class, CALLS_REAL_METHODS);

    when(underTest.sendMessage(1)).thenReturn(response1);
    when(underTest.sendMessage(2)).thenReturn(response2);
    when(underTest.sendMessage(3)).thenReturn(response3);

    List<StandardResponse> result = underTest.sendMessages(ImmutableList.of(1, 2, 3));

    assertThat(result.size(), is(3));
    assertThat(result.get(0), is(response1));
    assertThat(result.get(1), is(response2));
    assertThat(result.get(2), is(response3));

    InOrder inOrder = Mockito.inOrder(underTest);
    inOrder.verify(underTest).sendMessage(1);
    inOrder.verify(underTest).sendMessage(2);
    inOrder.verify(underTest).sendMessage(3);
  }

}
