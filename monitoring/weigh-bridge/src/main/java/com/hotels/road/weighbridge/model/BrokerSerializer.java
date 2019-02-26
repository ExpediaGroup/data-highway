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
package com.hotels.road.weighbridge.model;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

@Component
public class BrokerSerializer implements StreamSerializer<Broker> {

  @Autowired
  private ObjectMapper objectMapper;

  @Override
  public void write(ObjectDataOutput out, Broker broker) throws IOException {
    final byte[] b = objectMapper.writeValueAsBytes(broker);
    out.writeByteArray(b);
  }

  @Override
  public Broker read(ObjectDataInput in) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    final byte[] src = in.readByteArray();
    return mapper.readValue(src, Broker.class);
  }

  @Override
  public int getTypeId() {
    return 1;
  }

  @Override
  public void destroy() {}
}
