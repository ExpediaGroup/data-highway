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
package com.hotels.road.loadingbay;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.loadingbay.model.HiveRoad;

@RunWith(MockitoJUnitRunner.class)
public class HiveModelReaderTest {

  @Mock
  private ObjectMapper mapper;
  @Mock
  private JsonNode json;
  @Mock
  private HiveRoad road;

  private HiveModelReader underTest;

  @Before
  public void before() {
    underTest = new HiveModelReader(mapper);
  }

  @Test
  public void test() {
    when(mapper.convertValue(json, HiveRoad.class)).thenReturn(road);

    HiveRoad result = underTest.read(json);

    assertThat(result, is(road));
  }

}
