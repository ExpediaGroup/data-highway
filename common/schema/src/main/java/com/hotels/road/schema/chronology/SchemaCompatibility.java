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
package com.hotels.road.schema.chronology;

import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import org.apache.avro.Schema;

import lombok.RequiredArgsConstructor;

import com.hotels.avro.compatibility.ChronologyCompatibilityCheckResult;
import com.hotels.avro.compatibility.Compatibility;

@RequiredArgsConstructor(access = PRIVATE)
public enum SchemaCompatibility {
  CAN_READ_ALL(Compatibility.Mode.CAN_READ_ALL),
  CAN_READ_LATEST(Compatibility.Mode.CAN_READ_LATEST),
  CAN_BE_READ_BY_ALL(Compatibility.Mode.CAN_BE_READ_BY_ALL),
  CAN_BE_READ_BY_LATEST(Compatibility.Mode.CAN_BE_READ_BY_LATEST),
  MUTUAL_READ_ALL(Compatibility.Mode.MUTUAL_READ_WITH_ALL),
  MUTUAL_READ_LATEST(Compatibility.Mode.MUTUAL_READ_WITH_LATEST);

  private final Compatibility.Mode compatibilityMode;

  /**
   * @param toValidate the {@link Schema} to validate.
   * @param existing the chronology of historical {@link Schema Schemas} by id.
   * @throws SchemaCompatibilityException if a validation error occurs.
   */
  public void validate(Schema toValidate, Map<Integer, Schema> existing) throws SchemaCompatibilityException {
    ChronologyCompatibilityCheckResult compatibilityResult = compatibilityMode.check(toValidate, existing.values());
    if (!compatibilityResult.isCompatible()) {
      throw new SchemaCompatibilityException(compatibilityResult.asMessage());
    }
  }
}
