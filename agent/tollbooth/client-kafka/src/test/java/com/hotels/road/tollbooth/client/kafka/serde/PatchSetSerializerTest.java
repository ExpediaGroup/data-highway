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
package com.hotels.road.tollbooth.client.kafka.serde;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.hotels.road.tollbooth.client.api.PatchSet;

@RunWith(MockitoJUnitRunner.class)
public class PatchSetSerializerTest {

  @Mock
  private ObjectWriter objectWriter;
  @Mock
  private PatchSet patch;

  private PatchSetSerializer underTest;

  @Before
  public void before() {
    underTest = new PatchSetSerializer(objectWriter);
  }

  @Test
  public void configure() {
    underTest.configure(Collections.emptyMap(), false);

    verifyZeroInteractions(objectWriter);
  }

  @Test
  public void deserialize() throws IOException {
    byte[] data = new byte[] { 0 };

    when(objectWriter.writeValueAsBytes(patch)).thenReturn(data);

    byte[] result = underTest.serialize("topic", patch);

    assertThat(result, is(data));
  }

  @Test(expected = RuntimeException.class)
  public void deserializeThrowsException() throws Exception {
    doThrow(JsonProcessingException.class).when(objectWriter).writeValueAsBytes(patch);

    underTest.serialize("topic", patch);
  }

  @Test
  public void defaultMapper() throws Exception {
    try (PatchSetSerializer serializer = new PatchSetSerializer()) {
      PatchSet patchSet = new PatchSet("road1", new ArrayList<>());
      byte[] bytes = serializer.serialize("topic", patchSet);
      ObjectReader reader = new ObjectMapper().readerFor(PatchSet.class);
      assertThat(reader.readValue(bytes), is(patchSet));
    }
  }

  @Test
  public void close() {
    underTest.close();

    verifyZeroInteractions(objectWriter);
  }

}
