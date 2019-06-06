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
package com.hotels.road.truck.park.decoder.gdpr;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.hotels.road.pii.PiiReplacer;

@Component
@RequiredArgsConstructor
public class PiiStringConversion extends Conversion<String> {
  private final PiiReplacer replacer;

  @Override
  public Class<String> getConvertedType() {
    return String.class;
  }

  @Override
  public String getLogicalTypeName() {
    return "pii-string";
  }

  @Override
  public String fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return replacer.replace(value.toString());
  }
}
