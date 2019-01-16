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
package com.hotels.road.truck.park.decoder.gdpr;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.hotels.road.truck.park.decoder.gdpr.PiiBytesConversion;

public class PiiBytesConversionTest {

  @Test
  public void testName() throws Exception {
    PiiBytesConversion underTest = new PiiBytesConversion();
    assertThat(underTest.getConvertedType(), is(equalTo(ByteBuffer.class)));
    assertThat(underTest.getLogicalTypeName(), is("pii-bytes"));
    assertThat(underTest.fromBytes(ByteBuffer.wrap(new byte[] { 0 }), null, null), is(PiiBytesConversion.EMPTY));
  }
}
