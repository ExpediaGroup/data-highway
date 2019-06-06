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
package com.hotels.road.onramp.kafka;

import static java.util.Collections.singletonMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.onramp.api.Onramp;

@RunWith(MockitoJUnitRunner.class)
public class OnrampServiceImplTest {

  private static final String NON_EXISTENT_ROAD = "NON-EXISTENT_ROAD";
  private static final String TEST_ROAD = "test-road";
  private static final Schema SCHEMA = SchemaBuilder
      .builder()
      .record("r")
      .fields()
      .name("f")
      .type()
      .stringType()
      .noDefault()
      .endRecord();

  @Mock
  private KafkaStore<String, Road> roadsMock;

  @Mock
  private OnrampMetrics metrics;

  private Producer<byte[], byte[]> producerMock;
  private OnrampServiceImpl underTest;

  @Before
  public void setUp() {
    underTest = new OnrampServiceImpl(metrics, roadsMock, producerMock);
  }

  @Test
  public void nonExistentRoad() throws Exception {
    when(roadsMock.get(NON_EXISTENT_ROAD)).thenReturn(null);
    Optional<Onramp> onramp = underTest.getOnramp(NON_EXISTENT_ROAD);
    assertFalse(onramp.isPresent());
  }

  @Test(expected = RuntimeException.class)
  public void storeException() throws Exception {
    when(roadsMock.get(TEST_ROAD)).thenThrow(RuntimeException.class);
    underTest.getOnramp(TEST_ROAD);
  }

  @Test
  public void roadFound() throws Exception {
    Road road = new Road();
    road.setSchemas(singletonMap(1, new SchemaVersion(SCHEMA, 1, false)));
    when(roadsMock.get(TEST_ROAD)).thenReturn(road);
    Optional<Onramp> onramp = underTest.getOnramp(TEST_ROAD);
    assertTrue(onramp.isPresent());
  }

}
