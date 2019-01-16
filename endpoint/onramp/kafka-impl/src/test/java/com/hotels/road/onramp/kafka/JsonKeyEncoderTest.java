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
package com.hotels.road.onramp.kafka;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.partition.KeyPathParser;

@RunWith(MockitoJUnitRunner.class)
public class JsonKeyEncoderTest {

  private static final int RANDOM_VALUE = 42;

  private @Mock Random random;
  private JsonNode message;

  @Before
  public void initialiseMocks() throws Exception {
    when(random.nextInt()).thenReturn(RANDOM_VALUE);
    message = new ObjectMapper().readTree("{\"a\":\"b\",\"c\":null}");
  }

  @Test
  public void noPath() {
    JsonKeyEncoder encoder = new JsonKeyEncoder(() -> null, random);
    assertThat(encoder.encode(message), is(asBytes(RANDOM_VALUE)));
  }

  @Test
  public void pathResolvesToMissingInMessage() {
    JsonKeyEncoder encoder = new JsonKeyEncoder(() -> KeyPathParser.parse("$.X"), random);
    assertThat(encoder.encode(message), is(asBytes(RANDOM_VALUE)));
  }

  @Test
  public void pathResolvesToNullInMessage() {
    JsonKeyEncoder encoder = new JsonKeyEncoder(() -> KeyPathParser.parse("$.c"), random);
    assertThat(encoder.encode(message), is(asBytes(message.get("c").hashCode())));
  }

  @Test
  public void path() {
    JsonKeyEncoder encoder = new JsonKeyEncoder(() -> KeyPathParser.parse("$.a"), random);
    assertThat(encoder.encode(message), is(asBytes(message.get("a").hashCode())));
  }

  private static byte[] asBytes(int value) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
  }
}
