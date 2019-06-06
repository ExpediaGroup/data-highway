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
package com.hotels.road.pii;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class DefaultPiiReplacerTest {
  private final DefaultPiiReplacer underTest = new DefaultPiiReplacer();

  @Test
  public void fooInput() throws Exception {
    String result = underTest.replace("foo");
    assertThat(result, is(""));
  }

  @Test
  public void nullInput() throws Exception {
    String result = underTest.replace(null);
    assertThat(result, is(nullValue()));
  }
}
