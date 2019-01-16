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
package com.hotels.road.truck.park;

import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.LongConsumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerCountOutputStreamTest {

  @Mock
  private OutputStream delegate;
  @Mock
  private LongConsumer consumer;

  private ConsumerCountOutputStream underTest;

  @Before
  public void before() {
    underTest = new ConsumerCountOutputStream(delegate, consumer);
  }

  @Test
  public void writeArray() throws IOException {
    byte[] b = new byte[] { 0, 1, 2 };
    int off = 0;
    int len = 2;
    underTest.write(b, off, len);

    verify(delegate).write(b, off, len);
    verify(consumer).accept(len);
  }

  @Test
  public void write() throws IOException {
    underTest.write(0);

    verify(delegate).write(0);
    verify(consumer).accept(1);
  }

}
