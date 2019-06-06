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
package com.hotels.road.schema.validation;

import static org.junit.Assert.fail;

import static com.hotels.road.schema.gdpr.PiiVisitor.PII;
import static com.hotels.road.schema.gdpr.PiiVisitor.SENSITIVITY;
import static com.hotels.road.schema.validation.DataHighwaySchemaValidator.PARTITION_COLUMN_NAME;
import static com.hotels.road.schema.validation.DataHighwaySchemaValidator.UnionRule.ALLOW_NON_NULLABLE_UNIONS;
import static com.hotels.road.schema.validation.DataHighwaySchemaValidator.UnionRule.DISALLOW_NON_NULLABLE_UNIONS;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import com.hotels.jasvorno.schema.SchemaValidationException;
import com.hotels.road.schema.gdpr.InvalidPiiAnnotationException;

public class DataHighwaySchemaValidatorTest {

  @Test(expected = SchemaValidationException.class)
  public void rootTypeIsNotRecord() throws Exception {
    Schema schema = SchemaBuilder.builder().bytesType();
    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = SchemaValidationException.class)
  public void schemaContainsFieldWithReservedColumnName() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name(PARTITION_COLUMN_NAME)
        .type()
        .stringType()
        .noDefault()
        .endRecord();
    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = SchemaValidationException.class)
  public void schemaContainsAliasWithReservedColumnName() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .aliases(PARTITION_COLUMN_NAME)
        .type()
        .stringType()
        .noDefault()
        .endRecord();
    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test
  public void schemaDoesNotContainReservedColumnName() throws Exception {
    Schema schema = SchemaBuilder.record("r").fields().name("f").type().stringType().noDefault().endRecord();
    try {
      DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
    } catch (SchemaValidationException e) {
      fail();
    }
  }

  @Test
  public void nullableUnion() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().and().nullType().endUnion();
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(union).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test
  public void optionalUnion() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().nullType().and().intType().endUnion();
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(union).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void singleTypeInUnion() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().endUnion();
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(union).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test
  public void singleTypeInUnionAllow() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().endUnion();
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(union).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, ALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void twoTypesInUnion() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().and().longType().endUnion();
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(union).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void threeTypesInUnion() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().and().longType().and().nullType().endUnion();
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(union).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test
  public void validUnionInMap() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().and().nullType().endUnion();
    Schema map = SchemaBuilder.map().values(union);
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(map).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidUnionInMap() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().and().longType().endUnion();
    Schema map = SchemaBuilder.map().values(union);
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(map).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test
  public void validUnionInArray() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().and().nullType().endUnion();
    Schema array = SchemaBuilder.array().items(union);
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(array).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidUnionInArray() throws SchemaValidationException {
    Schema union = SchemaBuilder.unionOf().intType().and().longType().endUnion();
    Schema array = SchemaBuilder.array().items(union);
    Schema schema = SchemaBuilder.record("r").fields().name("f").type(array).noDefault().endRecord();

    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test
  public void validPiiProperty() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop(SENSITIVITY, PII)
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();
    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = InvalidPiiAnnotationException.class)
  public void invalidPiiProperty() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop(SENSITIVITY, PII)
        .type(SchemaBuilder.builder().map().values().stringType())
        .noDefault()
        .endRecord();
    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidLogicalType() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop(LogicalType.LOGICAL_TYPE_PROP, "date")
        .type(SchemaBuilder.builder().map().values().stringType())
        .noDefault()
        .endRecord();
    DataHighwaySchemaValidator.validate(schema, DISALLOW_NON_NULLABLE_UNIONS);
  }
}
