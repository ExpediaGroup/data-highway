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
package com.hotels.road.truck.park.decoder.gdpr;

import java.nio.ByteBuffer;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

@Component
public class PiiBytesConversion extends Conversion<ByteBuffer> {
  static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);

  @Override
  public Class<ByteBuffer> getConvertedType() {
    return ByteBuffer.class;
  }

  @Override
  public String getLogicalTypeName() {
    return "pii-bytes";
  }

  @Override
  public ByteBuffer fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
    return EMPTY;
  }
}
