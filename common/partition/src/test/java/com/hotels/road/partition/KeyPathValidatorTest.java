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
package com.hotels.road.partition;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import com.hotels.road.exception.InvalidKeyPathException;
import com.hotels.road.partition.KeyPathParser.Path;

public class KeyPathValidatorTest {

  @Test
  public void rootPath() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$");
    Schema schema = Schema.create(Type.STRING);
    new KeyPathValidator(path, schema).validate();
  }

  @Test
  public void nested() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$[\"a\"][\"b\"].c");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder
            .record("Y")
            .fields()
            .name("b")
            .type(SchemaBuilder.record("Z").fields().name("c").type(Schema.create(Type.STRING)).noDefault().endRecord())
            .noDefault()
            .endRecord())
        .noDefault()
        .endRecord();
    new KeyPathValidator(path, schema).validate();
  }

  @Test(expected = InvalidKeyPathException.class)
  public void mapFail() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$.a.b[\"c\"]");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.map().values(
            SchemaBuilder.record("Z").fields().name("c").type(Schema.create(Type.STRING)).noDefault().endRecord()))
        .noDefault()
        .endRecord();
    try {
      new KeyPathValidator(path, schema).validate();
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Element '$.a' not a traversable type (found 'MAP'), in path: $.a.b[\"c\"]"));
      throw e;
    }
  }

  @Test(expected = InvalidKeyPathException.class)
  public void arrayFail() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$.a.b.c");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.array().items(
            SchemaBuilder.record("Z").fields().name("c").type(Schema.create(Type.STRING)).noDefault().endRecord()))
        .noDefault()
        .endRecord();
    try {
      new KeyPathValidator(path, schema).validate();
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Element '$.a' not a traversable type (found 'ARRAY'), in path: $.a.b.c"));
      throw e;
    }
  }

  @Test
  public void unionAllBranchesOk() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$[\"a\"][\"c\"]");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder
            .unionOf()
            .type(SchemaBuilder.record("Z").fields().name("c").type(Schema.create(Type.STRING)).noDefault().endRecord())
            .and()
            .type(SchemaBuilder.record("K").fields().name("c").type(Schema.create(Type.INT)).noDefault().endRecord())
            .endUnion())
        .noDefault()
        .endRecord();
    new KeyPathValidator(path, schema).validate();
  }

  @Test(expected = InvalidKeyPathException.class)
  public void unionBranchIncompatible() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$.a.c");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder
            .unionOf()
            .type(SchemaBuilder.record("Z").fields().name("c").type(Schema.create(Type.STRING)).noDefault().endRecord())
            .and()
            .type(Schema.create(Type.INT))
            .endUnion())
        .noDefault()
        .endRecord();
    try {
      new KeyPathValidator(path, schema).validate();
    } catch (Exception e) {
      assertThat(e.getMessage(),
          is("Element '$.a(UnionIndex:1)' not a traversable type (found 'INT'), in path: $.a.c"));
      throw e;
    }
  }

  @Test
  public void smallerNested() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$[\"a\"][\"b\"]");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.record("Y").fields().name("b").type(Schema.create(Type.STRING)).noDefault().endRecord())
        .noDefault()
        .endRecord();
    new KeyPathValidator(path, schema).validate();
  }

  @Test
  public void smallestNested() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$[\"a\"]");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(Schema.create(Type.STRING))
        .noDefault()
        .endRecord();
    new KeyPathValidator(path, schema).validate();
  }

  @Test(expected = InvalidKeyPathException.class)
  public void rootNotARecord() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$[\"a\"]");
    Schema schema = SchemaBuilder.array().items(Schema.create(Type.STRING));
    try {
      new KeyPathValidator(path, schema).validate();
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Element '$' not a traversable type (found 'ARRAY'), in path: $[\"a\"]"));
      throw e;
    }
  }

  @Test(expected = InvalidKeyPathException.class)
  public void nestedFail() throws InvalidKeyPathException {
    Path path = KeyPathParser.parse("$[\"a\"].b[\"c\"]");
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.record("Y").fields().name("b").type(Schema.create(Type.STRING)).noDefault().endRecord())
        .noDefault()
        .endRecord();
    try {
      new KeyPathValidator(path, schema).validate();
    } catch (Exception e) {
      assertThat(e.getMessage(),
          is("Element '$[\"a\"].b' not a traversable type (found 'STRING'), in path: $[\"a\"].b[\"c\"]"));
      throw e;
    }
  }

}
