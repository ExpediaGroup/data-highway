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
package com.hotels.road.truck.park.decoder.gdpr;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.pii.PiiReplacer;

@RunWith(MockitoJUnitRunner.class)
public class PiiStringConversionTest {
  private @Mock PiiReplacer replacer;

  private PiiStringConversion underTest;

  @Before
  public void before() throws Exception {
    underTest = new PiiStringConversion(replacer);
  }

  @Test
  public void idMethods() throws Exception {
    assertThat(underTest.getConvertedType(), is(equalTo(String.class)));
    assertThat(underTest.getLogicalTypeName(), is("pii-string"));
  }

  @Test
  public void hash() throws Exception {
    doReturn("bar").when(replacer).replace("foo");
    String result = underTest.fromCharSequence("foo", null, null);
    assertThat(result, is("bar"));
  }
}
