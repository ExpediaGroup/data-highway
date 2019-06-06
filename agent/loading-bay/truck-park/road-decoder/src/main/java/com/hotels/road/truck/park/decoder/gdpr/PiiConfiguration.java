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

import org.apache.avro.generic.GenericData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hotels.road.pii.PiiReplacerConfiguration;

@Configuration
@Import(PiiReplacerConfiguration.class)
public class PiiConfiguration {
  @Bean
  public GenericData genericData(PiiStringConversion stringConversion, PiiBytesConversion bytesConversion) {
    GenericData genericData = new GenericData();
    genericData.addLogicalTypeConversion(stringConversion);
    genericData.addLogicalTypeConversion(bytesConversion);
    return genericData;
  }
}
