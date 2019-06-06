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
package com.hotels.road.rest.controller.common;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletInputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServletInputStreamWrapperTest {

  @Mock
  private InputStream content;

  private ServletInputStream underTest;

  @Before
  public void before() {
    underTest = new ServletInputStreamWrapper(content);
  }

  @Test
  public void read() throws IOException {
    when(content.read()).thenReturn(1);

    int result = underTest.read();

    assertThat(result, is(1));
  }

  @Test
  public void isFinished() throws IOException {
    when(content.available()).thenReturn(0);

    boolean result = underTest.isFinished();

    assertThat(result, is(true));
  }

  @Test
  public void isFinished_Not() throws IOException {
    when(content.available()).thenReturn(1);

    boolean result = underTest.isFinished();

    assertThat(result, is(false));
  }

  @Test(expected = RuntimeException.class)
  public void isFinished_Throws() throws IOException {
    doThrow(IOException.class).when(content).available();

    underTest.isFinished();
  }

  @Test
  public void isReady() {
    boolean result = underTest.isReady();

    assertThat(result, is(true));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void setReadListener() {
    underTest.setReadListener(null);
  }

}
