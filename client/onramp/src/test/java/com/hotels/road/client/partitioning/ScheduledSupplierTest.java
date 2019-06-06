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
package com.hotels.road.client.partitioning;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScheduledSupplierTest {

  private @Mock Supplier<String> delegate;
  private @Mock ScheduledExecutorService executor;
  private @Captor ArgumentCaptor<Runnable> captureUpdate;

  @Before
  public void initialiseMocks() {
    when(delegate.get()).thenReturn("VALUE1", "VALUE2", "VALUE3");
  }

  @Test
  public void unscheduledInitialCall() throws Exception {
    ScheduledSupplier<String> supplier = new ScheduledSupplier<>(delegate, executor, 1, SECONDS);
    assertThat(supplier.get(), is("VALUE1"));
    supplier.close();
  }

  @Test
  public void checkScheduling() throws Exception {
    ScheduledSupplier<String> supplier = new ScheduledSupplier<>(delegate, executor, 1, SECONDS);
    Mockito.verifyZeroInteractions(executor);
    supplier.get();
    verify(executor).scheduleWithFixedDelay(captureUpdate.capture(), eq(1L), eq(1L), eq(SECONDS));
    supplier.close();
  }

  @Test
  public void checkClose() throws Exception {
    ScheduledSupplier<String> supplier = new ScheduledSupplier<>(delegate, executor, 1, SECONDS);
    supplier.close();
    verify(executor).shutdownNow();
    verify(executor).awaitTermination(10, SECONDS);
  }

  @Test
  public void update() throws Exception {
    ScheduledSupplier<String> supplier = new ScheduledSupplier<>(delegate, executor, 1, SECONDS);
    assertThat(supplier.get(), is("VALUE1"));
    verify(executor).scheduleWithFixedDelay(captureUpdate.capture(), eq(1L), eq(1L), eq(SECONDS));
    Runnable runnable = captureUpdate.getValue();
    runnable.run();
    assertThat(supplier.get(), is("VALUE2"));
    runnable.run();
    assertThat(supplier.get(), is("VALUE3"));
    supplier.close();
  }

}
