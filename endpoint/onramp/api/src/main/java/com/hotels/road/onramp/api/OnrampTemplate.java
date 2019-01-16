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

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.InvalidKeyException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.rest.model.Authorisation;

/**
 * Business logic level implementation of an {@link Onramp}.
 */
public abstract class OnrampTemplate<K, M> implements Onramp {

  private final Road road;

  protected OnrampTemplate(Road road) {
    this.road = road;
  }

  @Override
  public Future<Boolean> sendEvent(JsonNode jsonEvent) {
    try {
      SchemaVersion schemaVersion = getSchemaVersion();
      Event<K, M> event = encodeEvent(jsonEvent, schemaVersion);
      return sendEncodedEvent(event, schemaVersion);
    } catch (InvalidEventException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public boolean isAvailable() {
    return road.isEnabled();
  }

  @Override
  public List<String> getCidrBlocks() {
    return Optional
        .of(getRoad())
        .map(Road::getAuthorisation)
        .map(Authorisation::getOnramp)
        .map(Authorisation.Onramp::getCidrBlocks)
        .filter(l -> l.size() > 0)
        .orElseGet(() -> singletonList("0.0.0.0/0"));
  }

  protected Road getRoad() {
    return road;
  }

  abstract protected Event<K, M> encodeEvent(JsonNode jsonEvent, SchemaVersion schemaVersion)
    throws InvalidEventException;

  abstract protected Future<Boolean> sendEncodedEvent(Event<K, M> event, SchemaVersion schemaVersion)
    throws InvalidKeyException;

}
