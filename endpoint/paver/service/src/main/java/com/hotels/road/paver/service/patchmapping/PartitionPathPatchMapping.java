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
package com.hotels.road.paver.service.patchmapping;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.hotels.road.exception.InvalidKeyPathException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;
import com.hotels.road.partition.KeyPathValidator;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@Component
public class PartitionPathPatchMapping extends PatchMapping {

  @Override
  public String getPath() {
    return "/partitionPath";
  }

  @Override
  public PatchOperation convertOperation(Road road, PatchOperation modelOperation) {
    checkArgument(isAddOrReplace(modelOperation), "You can only change the value of partitionPath");
    checkArgument(modelOperation.getValue() instanceof String, "partitionPath must be a string");
    checkArgument(StringUtils.isNotBlank((CharSequence) modelOperation.getValue()) || modelOperation.getValue() == null,
        "partitionPath cannot be blank, it must be null or have a value");

    Path path = KeyPathParser.parse((String) modelOperation.getValue());
    Map<Integer, SchemaVersion> schemas = road.getSchemas();
    Integer latestSchemaVersion = schemas.keySet().stream().max(Integer::compareTo).orElse(null);
    if (latestSchemaVersion != null) {
      try {
        new KeyPathValidator(path, schemas.get(latestSchemaVersion).getSchema()).validate();
      } catch (InvalidKeyPathException e) {
        throw new IllegalArgumentException(String.format(
            "partitionPath '%s' not compatible with latest schema (version %d).", path, latestSchemaVersion), e);
      }
    }

    return PatchOperation.replace("/partitionPath", modelOperation.getValue());
  }
}
