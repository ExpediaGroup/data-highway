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

@RunWith(MockitoJUnitRunner.class)
public class MessageHashCodeFunctionSupplierTest {

  private static final int RANDOM_VALUE = 42;
  private @Mock Random random;
  private JsonNode message;

  @Before
  public void initialiseMocks() throws Exception {
    when(random.nextInt()).thenReturn(RANDOM_VALUE);
    message = new ObjectMapper().readTree("{\"a\":\"b\"}");
  }

  @Test
  public void randomOnNullPath() {
    @SuppressWarnings("resource")
    Function<JsonNode, Integer> fn = new MessageHashCodeFunctionSupplier(() -> null, random).get();
    assertThat(fn.apply(message), is(RANDOM_VALUE));
  }

  @Test
  public void jsonNodeHashcode() throws Exception {
    @SuppressWarnings("resource")
    Function<JsonNode, Integer> fn = new MessageHashCodeFunctionSupplier(() -> KeyPathParser.parse("$.a"), random)
        .get();
    assertThat(fn.apply(message), is(message.get("a").hashCode()));
  }

}
