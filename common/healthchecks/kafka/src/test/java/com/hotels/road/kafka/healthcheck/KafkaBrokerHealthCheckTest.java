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
package com.hotels.road.kafka.healthcheck;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.health.HealthCheck.Result;

@RunWith(MockitoJUnitRunner.class)
public class KafkaBrokerHealthCheckTest {

  @Mock
  private Consumer<String, String> consumer;

  @Mock
  private KafkaConsumerFactory kafkaConsumerFactory;

  @Mock
  private ExecutorService executorService;

  @Mock
  private Future<Result> future;

  private KafkaBrokerHealthCheck underTest;

  @Before
  public void setUp() {
    when(kafkaConsumerFactory.create("healthcheck")).thenReturn(consumer);
    underTest = new KafkaBrokerHealthCheck(consumer, executorService);
  }

  @Test
  public void typical() throws Exception {
    underTest = new KafkaBrokerHealthCheck(kafkaConsumerFactory);
    assertThat(underTest.check().isHealthy(), is(true));
    underTest.close();
  }

  @Test
  public void kafkaException() throws Exception {
    when(kafkaConsumerFactory.create("healthcheck")).thenReturn(consumer);
    underTest = new KafkaBrokerHealthCheck(kafkaConsumerFactory);
    KafkaException kafkaException = new KafkaException();
    when(consumer.listTopics()).thenThrow(kafkaException);
    assertThat(underTest.check().isHealthy(), is(false));
    underTest.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void futureTimeout() throws Exception {
    when(executorService.submit(any(Callable.class))).thenReturn(future);
    doThrow(TimeoutException.class).when(future).get(5, TimeUnit.SECONDS);
    assertThat(underTest.check().isHealthy(), is(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executionException() throws Exception {
    ExecutionException e = new ExecutionException(new RuntimeException());
    when(executorService.submit(any(Callable.class))).thenReturn(future);
    doThrow(e).when(future).get(5, TimeUnit.SECONDS);
    assertThat(underTest.check().isHealthy(), is(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void interrupted() throws Exception {
    InterruptedException e = new InterruptedException();
    when(executorService.submit(any(Callable.class))).thenReturn(future);
    doThrow(e).when(future).get(5, TimeUnit.SECONDS);
    assertThat(underTest.check().isHealthy(), is(false));
  }

  @Test
  public void logAndIgnoreInterruptedExceptionWhenClosing() throws Exception {
    InterruptedException e = new InterruptedException();
    doThrow(e).when(executorService).awaitTermination(10, TimeUnit.SECONDS);
    underTest = new KafkaBrokerHealthCheck(consumer, executorService);
    underTest.close();
  }

}
