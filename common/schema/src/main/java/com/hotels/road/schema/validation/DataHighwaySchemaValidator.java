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
package com.hotels.road.schema.validation;

import static com.hotels.road.schema.validation.DataHighwaySchemaValidator.UnionRule.DISALLOW_NON_NULLABLE_UNIONS;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.hotels.jasvorno.schema.SchemaValidationException;
import com.hotels.jasvorno.schema.SchemaValidator;
import com.hotels.road.schema.SchemaTraverser;
import com.hotels.road.schema.gdpr.PiiVisitor;

public final class DataHighwaySchemaValidator {
  public enum UnionRule {
    ALLOW_NON_NULLABLE_UNIONS,
    DISALLOW_NON_NULLABLE_UNIONS;
  }

  static final String PARTITION_COLUMN_NAME = "acquisition_instant"; // Also declared in road-hive-agent HiveUtilsImpl

  private DataHighwaySchemaValidator() {}

  /**
   * We don't allow {@code union[bytes, string[, ...]]}.
   */
  public static boolean isValid(Schema schema, UnionRule unionRule) {
    try {
      validate(schema, unionRule);
    } catch (SchemaValidationException e) {
      return false;
    }
    return true;
  }

  /**
   * We don't allow {@code union[bytes, string[, ...]]}.
   *
   * @throws SchemaValidationException
   */
  public static void validate(Schema schema, UnionRule unionRule) throws SchemaValidationException {
    validateIsRecord(schema);
    validateDoesNotContainReservedColumnName(schema);
    validateObfuscationAnnotations(schema);
    validateLogicalTypes(schema);
    SchemaValidator.validate(schema);
    if (DISALLOW_NON_NULLABLE_UNIONS == unionRule) {
      validateNonNullableUnions(schema);
    }
  }

  private static void validateIsRecord(Schema schema) throws SchemaValidationException {
    if (schema.getType() != Type.RECORD) {
      throw new SchemaValidationException(
          String.format("Unexpected schema root type '%s', expected '%s'", schema.getType(), Type.RECORD));
    }
  }

  private static void validateDoesNotContainReservedColumnName(Schema schema) throws SchemaValidationException {
    for (Field field : schema.getFields()) {
      if (PARTITION_COLUMN_NAME.equalsIgnoreCase(field.name())) {
        throw new SchemaValidationException(String.format("Field name '%s' is reserved", field.name()));
      }
      for (String alias : field.aliases()) {
        if (PARTITION_COLUMN_NAME.equalsIgnoreCase(alias)) {
          throw new SchemaValidationException(String.format("Field name '%s' is reserved", alias));
        }
      }
    }
  }

  private static void validateObfuscationAnnotations(Schema schema) throws SchemaValidationException {
    SchemaTraverser.traverse(schema, new PiiVisitor<Void>());
  }

  private static void validateLogicalTypes(Schema schema) throws SchemaValidationException {
    SchemaTraverser.traverse(schema, new LogicalTypeValidatingVisitor());
  }

  private static void validateNonNullableUnions(Schema schema) throws SchemaValidationException {
    SchemaTraverser.traverse(schema, new NonNullableUnionValidatingVisitor());
  }
}
