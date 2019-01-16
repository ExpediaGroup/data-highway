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
package com.hotels.road.testdrive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;

import com.hotels.jasvorno.JasvornoConverter;
import com.hotels.jasvorno.JasvornoConverterException;
import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.InvalidKeyException;
import com.hotels.road.exception.RoadUnavailableException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.onramp.api.Event;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampTemplate;

class MemoryOnramp extends OnrampTemplate<Void, JsonNode> implements Onramp {
  private final Map<String, List<Record>> messages;
  private final Road road;

  MemoryOnramp(Road road, Map<String, List<Record>> messages) {
    super(road);
    this.road = road;
    this.messages = messages;
  }

  @Override
  public SchemaVersion getSchemaVersion() {
    return SchemaVersion.latest(road.getSchemas().values()).orElseThrow(
        () -> new RoadUnavailableException(String.format("Road '%s' has no schema.", road.getName())));
  }

  @Override
  protected Event<Void, JsonNode> encodeEvent(JsonNode jsonEvent, SchemaVersion schemaVersion)
    throws InvalidEventException {
    try {
      JasvornoConverter.convertToAvro(jsonEvent, schemaVersion.getSchema());
      return new Event<>(null, jsonEvent);
    } catch (JasvornoConverterException e) {
      throw new InvalidEventException(e.getMessage());
    }
  }

  @Override
  protected Future<Boolean> sendEncodedEvent(Event<Void, JsonNode> event, SchemaVersion schemaVersion)
    throws InvalidKeyException {
    Payload<JsonNode> payload = new Payload<>((byte) 0, schemaVersion.getVersion(), event.getMessage());
    List<Record> messages = this.messages.computeIfAbsent(road.getName(), name -> new ArrayList<>());
    Record record = new Record(0, messages.size(), System.currentTimeMillis(), payload);
    messages.add(record);
    return Futures.immediateFuture(true);
  }
}
