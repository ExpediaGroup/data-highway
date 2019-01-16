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
package com.hotels.road.onramp.api;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.InvalidKeyException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Onramp;

@RunWith(MockitoJUnitRunner.class)
public class OnrampTemplateTest {
  @Mock
  private Schema schema;
  @Mock
  private JsonNode jsonEvent;
  @Mock
  private Event<String, String> event;

  private OnrampTemplate<String, String> underTest;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    underTest = mock(OnrampTemplate.class, CALLS_REAL_METHODS);
  }

  @Test
  public void sendEventSuccess() throws Exception {
    SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);
    when(underTest.getSchemaVersion()).thenReturn(schemaVersion);
    when(underTest.encodeEvent(jsonEvent, schemaVersion)).thenReturn(event);
    Future<Boolean> future = CompletableFuture.completedFuture(true);
    when(underTest.sendEncodedEvent(event, schemaVersion)).thenReturn(future);

    Future<Boolean> result = underTest.sendEvent(jsonEvent);

    assertThat(result, is(future));
  }

  @Test
  public void sendEventInvalidEvent() throws Exception {
    SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);
    when(underTest.getSchemaVersion()).thenReturn(schemaVersion);
    InvalidEventException invalidEventException = new InvalidEventException("invalid");
    doThrow(invalidEventException).when(underTest).encodeEvent(jsonEvent, schemaVersion);

    Future<Boolean> result = underTest.sendEvent(jsonEvent);

    try {
      result.get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(invalidEventException));
    }
  }

  @Test
  public void sendEventInvalidKey() throws Exception {
    SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);
    when(underTest.getSchemaVersion()).thenReturn(schemaVersion);
    when(underTest.encodeEvent(jsonEvent, schemaVersion)).thenReturn(event);
    InvalidKeyException invalidKeyException = new InvalidKeyException("invalid");
    doThrow(invalidKeyException).when(underTest).sendEncodedEvent(event, schemaVersion);

    Future<Boolean> result = underTest.sendEvent(jsonEvent);

    try {
      result.get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(invalidKeyException));
    }
  }

  @Test
  public void roadIsAvailable() {
    Road road = new Road();
    road.setEnabled(true);

    underTest = new TestOnrampTemplate<>(road);

    assertThat(underTest.isAvailable(), is(true));
    assertThat(underTest.getRoad(), is(road));
  }

  @Test
  public void roadIsNotAvailable() {
    Road road = new Road();
    road.setEnabled(false);

    underTest = new TestOnrampTemplate<>(road);

    assertThat(underTest.isAvailable(), is(false));
    assertThat(underTest.getRoad(), is(road));
  }

  @Test
  public void getCidrBlocks() {
    Road road = new Road();
    Authorisation authorisation = new Authorisation();
    Onramp onramp = new Onramp();
    List<String> cidrBlocks = singletonList("cidrBlock");
    onramp.setCidrBlocks(cidrBlocks);
    authorisation.setOnramp(onramp);
    road.setAuthorisation(authorisation);
    when(underTest.getRoad()).thenReturn(road);

    List<String> result = underTest.getCidrBlocks();

    assertThat(result, is(cidrBlocks));
  }

  @Test
  public void getCidrBlocksNull() {
    Road road = new Road();
    Authorisation authorisation = new Authorisation();
    Onramp onramp = new Onramp();
    onramp.setCidrBlocks(null);
    authorisation.setOnramp(onramp);
    road.setAuthorisation(authorisation);
    when(underTest.getRoad()).thenReturn(road);

    List<String> result = underTest.getCidrBlocks();

    assertThat(result, is(singletonList("0.0.0.0/0")));
  }

  @Test
  public void getCidrBlocksEmpty() {
    Road road = new Road();
    Authorisation authorisation = new Authorisation();
    Onramp onramp = new Onramp();
    onramp.setCidrBlocks(emptyList());
    authorisation.setOnramp(onramp);
    road.setAuthorisation(authorisation);
    when(underTest.getRoad()).thenReturn(road);

    List<String> result = underTest.getCidrBlocks();

    assertThat(result, is(singletonList("0.0.0.0/0")));
  }

  private static class TestOnrampTemplate<K, M> extends OnrampTemplate<K, M> {
    private TestOnrampTemplate(Road road) {
      super(road);
    }

    @Override
    public SchemaVersion getSchemaVersion() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Event<K, M> encodeEvent(JsonNode jsonEvent, SchemaVersion schemaVersion) throws InvalidEventException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Future<Boolean> sendEncodedEvent(Event<K, M> event, SchemaVersion schemaVersion)
      throws InvalidKeyException {
      throw new UnsupportedOperationException();
    }
  }
}
