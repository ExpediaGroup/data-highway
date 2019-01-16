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
package com.hotels.road.towtruck;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
public class TowtruckTest {

  private static final String KEY = "key";
  @Mock
  private Map<String, JsonNode> store;
  @Mock
  private ObjectMapper mapper;
  @Mock
  private Supplier<String> keySupplier;
  @Mock
  private Function<String, OutputStream> outputStreamFactory;
  @Mock
  private OutputStream outputStream;

  private Towtruck underTest;

  @Before
  public void before() {
    underTest = new Towtruck(store, mapper, keySupplier, outputStreamFactory);
  }

  @Test
  public void happyPath() throws IOException {
    when(keySupplier.get()).thenReturn(KEY);
    when(outputStreamFactory.apply(KEY)).thenReturn(outputStream);

    underTest.performBackup();

    InOrder inOrder = inOrder(mapper, keySupplier, outputStreamFactory, outputStream);
    inOrder.verify(keySupplier).get();
    inOrder.verify(outputStreamFactory).apply(KEY);
    inOrder.verify(mapper).writeValue(outputStream, store);
    inOrder.verify(outputStream).close();
  }

  @Test(expected = IOException.class)
  public void exception() throws IOException {
    when(keySupplier.get()).thenReturn(KEY);
    doThrow(IOException.class).when(outputStreamFactory).apply(KEY);

    underTest.performBackup();
  }

}
