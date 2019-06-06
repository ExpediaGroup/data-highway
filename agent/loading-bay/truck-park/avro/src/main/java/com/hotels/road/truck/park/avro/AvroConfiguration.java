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
package com.hotels.road.truck.park.avro;

import static org.apache.avro.file.CodecFactory.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.file.CodecFactory.DEFAULT_XZ_LEVEL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.file.DataFileConstants.XZ_CODEC;

import java.util.Optional;

import org.apache.avro.file.CodecFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
public class AvroConfiguration {

  @Bean
  CodecFactory codecFactory(
      @Value("${avroCodec.name:deflate}") String codecName,
      @Value("${avroCodec.level:3}") String compressionLevel) {
    switch (codecName) {
    case DEFLATE_CODEC:
      return CodecFactory.deflateCodec(level(compressionLevel, DEFAULT_DEFLATE_LEVEL));
    case XZ_CODEC:
      return CodecFactory.xzCodec(level(compressionLevel, DEFAULT_XZ_LEVEL));
    default:
      return CodecFactory.fromString(codecName);
    }
  }

  private int level(String level, int defaultLevel) {
    return Optional.ofNullable(level).map(Integer::parseInt).orElse(defaultLevel);
  }

}
