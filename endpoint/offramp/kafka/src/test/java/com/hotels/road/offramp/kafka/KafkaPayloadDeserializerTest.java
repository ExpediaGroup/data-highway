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
package com.hotels.road.offramp.kafka;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.hotels.road.offramp.api.Payload;

public class KafkaPayloadDeserializerTest {

  @SuppressWarnings("resource")
  @Test
  public void test() {
    byte[] data = new byte[] { 0, 0, 0, 0, 1, 2, 3, 4, 5 };

    Payload<byte[]> payload = new PayloadDeserializer().deserialize("topic", data);

    assertThat(payload.getFormatVersion(), is((byte) 0));
    assertThat(payload.getSchemaVersion(), is(1));
    assertThat(payload.getMessage(), is(new byte[] { 2, 3, 4, 5 }));
  }
}
