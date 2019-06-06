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
package com.hotels.road.s3.io;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.google.common.io.ByteStreams.toByteArray;

import java.io.IOException;

import org.junit.Test;

public class S3PartOutputStreamTest {

  @Test
  public void typical() throws IOException {
    try (S3PartOutputStream underTest = new S3PartOutputStream(3, 1)) {
      underTest.write("foo".getBytes(UTF_8));
      S3Part result = underTest.s3Part();
      assertThat(result.getNumber(), is(1));
      assertThat(result.getSize(), is(3));
      assertThat(result.getMd5(), is("rL0Y20zC+Fzt72VPzMSk2A=="));
      assertThat(toByteArray(result.getInputStream()), is(new byte[] { 102, 111, 111 }));
    }
  }

}
