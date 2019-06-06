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
package com.hotels.road.paver.service.patchmapping;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hotels.road.model.core.Road;
import com.hotels.road.security.CidrBlockValidator;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@Configuration
public class PatchMappingConfiguration {
  @Bean
  PatchMapping contactEmailPatchMapping() {
    return new DefaultStringPatchMapping("contactEmail");
  }

  @Bean
  PatchMapping descriptionPatchMapping() {
    return new DefaultStringPatchMapping("description");
  }

  @Bean
  PatchMapping teamNamePatchMapping() {
    return new DefaultStringPatchMapping("teamName");
  }

  @Bean
  PatchMapping authorisationOnrampCidrBlockPatchMapping(CidrBlockValidator validator) {
    return new ListPatchMapping("/authorisation/onramp/cidrBlocks", String.class) {
      @SuppressWarnings("unchecked")
      @Override
      public PatchOperation convertOperation(Road road, PatchOperation modelOperation) {
        PatchOperation result = super.convertOperation(road, modelOperation);
        validator.validate((List<String>) result.getValue());
        return result;
      }
    };
  }

  @Bean
  PatchMapping authorisationOnrampAuthoritiesPatchMapping() {
    return new ListPatchMapping("/authorisation/onramp/authorities", String.class);
  }
}
