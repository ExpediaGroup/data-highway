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
package com.hotels.road.offramp.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

import com.hotels.road.pii.PiiReplacer;

@RunWith(MockitoJUnitRunner.class)
public class JsonNodePiiReplacerTest {
  private static final String EXAMPLE_STRING = "exampleString";
  private @Mock PiiReplacer replacer;
  private JsonNodePiiReplacer underTest;

  @Before
  public void setUp() throws Exception {
    underTest = new JsonNodePiiReplacer(replacer);
  }

  @Test
  public void binaryNode() throws Exception {
    JsonNode jsonNode = underTest.apply(BinaryNode.valueOf(new byte[] { 1, 0, 1, 2 }));
    assertThat(jsonNode.isBinary(), is(true));
    assertThat(jsonNode.binaryValue().length, is(0));
  }

  @Test
  public void textNode() throws Exception {
    doReturn("replaced").when(replacer).replace(EXAMPLE_STRING);
    JsonNode jsonNode = underTest.apply(TextNode.valueOf(EXAMPLE_STRING));
    assertThat(jsonNode.asText(), is("replaced"));
  }

  @Test
  public void nullNode() throws Exception {
    JsonNode jsonNode = underTest.apply(NullNode.getInstance());
    assertThat(jsonNode.isNull(), is(true));
  }

  @Test
  public void longNode() throws Exception {
    JsonNode jsonNode = underTest.apply(LongNode.valueOf(0L));
    assertThat(jsonNode.isLong(), is(true));
    assertThat(jsonNode.longValue(), is(0L));
  }
}
