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
package com.hotels.road.kafkastore.serialization;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class StringSerializerTest {
  public StringSerializer serializer = new StringSerializer();

  @Test
  public void keySerializationTest() throws Exception {
    assertThat(serializer.serializeKey(""), is(new byte[] { 0x01 }));
    assertThat(serializer.serializeKey("A"), is(new byte[] { 0x01, 'A' }));
    assertThat(serializer.serializeKey(null), is(new byte[] { 0x02 }));
  }

  @Test
  public void keyDeserializationTest() throws Exception {
    assertThat(serializer.deserializeKey(new byte[] { 0x01 }), is(""));
    assertThat(serializer.deserializeKey(new byte[] { 0x01, 'A' }), is("A"));
    assertThat(serializer.deserializeKey(new byte[] { 0x02 }), is(nullValue()));
  }

  @Test
  public void ValueSerializationTest() throws Exception {
    assertThat(serializer.serializeValue(""), is(new byte[] { 0x01 }));
    assertThat(serializer.serializeValue("A"), is(new byte[] { 0x01, 'A' }));
    assertThat(serializer.serializeValue(null), is(new byte[] { 0x02 }));
  }

  @Test
  public void ValueDeserializationTest() throws Exception {
    assertThat(serializer.deserializeValue(new byte[] { 0x01 }), is(""));
    assertThat(serializer.deserializeValue(new byte[] { 0x01, 'A' }), is("A"));
    assertThat(serializer.deserializeValue(new byte[] { 0x02 }), is(nullValue()));
  }
}
