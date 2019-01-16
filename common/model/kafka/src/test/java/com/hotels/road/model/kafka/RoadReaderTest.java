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
package com.hotels.road.model.kafka;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.UncheckedIOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.hotels.road.model.core.Road;

@RunWith(MockitoJUnitRunner.class)
public class RoadReaderTest {
  private final Road road = new Road();
  private final ObjectNode emptyNode = new ObjectMapper().createObjectNode();

  private @Mock ObjectMapper objectMapper;
  private @Mock ObjectReader reader;
  private @Mock JsonProcessingException jsonProcessingException;

  private RoadReader underTest;

  @Before
  public void setUp() {
    when(objectMapper.readerFor(Road.class)).thenReturn(reader);
    underTest = new RoadReader(objectMapper);
  }

  @Test
  public void defaultConstructor() {
    RoadReader underTest = new RoadReader();
    assertThat(underTest.read(emptyNode), is(road));
  }

  @Test(expected = UncheckedIOException.class)
  public void readFails() throws Exception {
    when(reader.readValue(emptyNode)).thenThrow(jsonProcessingException);
    underTest.read(emptyNode);
  }

}
