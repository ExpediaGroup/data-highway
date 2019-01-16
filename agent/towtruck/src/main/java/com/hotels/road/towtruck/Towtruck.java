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
package com.hotels.road.towtruck;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
@Slf4j
class Towtruck {
  private final Map<String, JsonNode> store;
  private final ObjectMapper mapper;
  private final Supplier<String> keySupplier;
  private final Function<String, OutputStream> outputStreamFactory;

  @Scheduled(cron = "0 0 * * * *")
  void performBackup() throws IOException {
    log.info("Starting backup.");
    try (OutputStream output = outputStreamFactory.apply(keySupplier.get())) {
      mapper.writeValue(output, store);
    }
    log.info("Backup completed.");
  }

}
