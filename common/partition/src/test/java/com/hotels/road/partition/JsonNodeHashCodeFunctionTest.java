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
package com.hotels.road.partition;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Random;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;

@RunWith(MockitoJUnitRunner.class)
public class JsonNodeHashCodeFunctionTest {

  private static final int RANDOM_VALUE = 42;

  private @Mock Random random;
  private Function<JsonNode, Integer> fn;

  @Before
  public void initialiseMocks() {
    when(random.nextInt()).thenReturn(RANDOM_VALUE);
    fn = new JsonNodeHashCodeFunction(random);
  }

  @Test
  public void randomOnMissingNode() {
    assertThat(fn.apply(MissingNode.getInstance()), is(RANDOM_VALUE));
  }

  @Test
  public void nodeHashCode() throws Exception {
    JsonNode node = new ObjectMapper().readTree("{\"a\":\"b\"}");
    assertThat(fn.apply(node), is(node.hashCode()));
  }

  @Test
  public void nullNodeHashCode() {
    assertThat(fn.apply(NullNode.getInstance()), is(NullNode.getInstance().hashCode()));
  }

}
