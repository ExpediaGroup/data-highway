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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.hotels.road.exception.InvalidKeyPathException;
import com.hotels.road.partition.KeyPathParser.Element;
import com.hotels.road.partition.KeyPathParser.Path;

/**
 * Validates that the supplied period delimited path can be used to locate a node in an Avro {@link Schema} and related
 * Avro data object. With the exception of {@link Type#NULL NULL}, any Avro type can be used as a key. Note that by
 * design individual elements contained within {@link Type#ARRAY ARRAYs} and {@link Type#MAP MAPs} cannot be addressed
 * as these are only verifiable at runtime, subject to the context of the message being sent.
 * <p>
 * If the path is empty, blank, or null then the entire message is used as the key.
 */
public class KeyPathValidator {

  private final Schema rootSchema;
  private final Path path;

  /**
   * @param path JSON path, where each path element describes a field within a {@link Type#RECORD RECORD}.
   * @param schema The message {@link Schema} in which the path describes the location of the key node.
   */
  public KeyPathValidator(Path path, Schema schema) {
    this.path = path;
    rootSchema = schema;
  }

  /** Validates that the path is correct with respect to the schema. */
  public void validate() throws InvalidKeyPathException {
    List<Element> elements = path.elements();
    validateElement(elements, rootSchema, elements.get(0).toString());
  }

  private void validateElement(List<Element> elements, Schema schema, String location) throws InvalidKeyPathException {
    if (elements.size() == 1) {
      return;
    }
    // This isn't the leaf of the expression so this schema must be a record or a union
    switch (schema.getType()) {
    case RECORD:
      validateRecordElement(elements, schema, location);
      break;
    case UNION:
      validateUnionElement(elements, schema, location);
      break;
    default:
      throw new InvalidKeyPathException(String.format("Element '%s' not a traversable type (found '%s'), in path: %s",
          location, schema.getType(), path));
    }
  }

  private void validateUnionElement(List<Element> elements, Schema schema, String location)
    throws InvalidKeyPathException {
    // Check all branches of the union are valid with respect to the path
    int unionIndex = 0;
    for (Schema unionBranch : schema.getTypes()) {
      validateElement(elements, unionBranch, location + "(UnionIndex:" + unionIndex + ")");
      unionIndex++;
    }
  }

  private void validateRecordElement(List<Element> elements, Schema schema, String location)
    throws InvalidKeyPathException {
    // Find the field referenced in the path from the record
    String fieldName = elements.get(1).id();
    Field field = schema.getField(fieldName);
    if (field == null) {
      throw new InvalidKeyPathException(
          String.format("Field '%s' does not exist in record located at '%s' in path: %s", fieldName, location, path));
    }
    validateElement(allButFirst(elements), field.schema(), location + elements.get(1).toString());
  }

  private List<Element> allButFirst(List<Element> elements) {
    return elements.subList(1, elements.size());
  }

}
