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
package com.hotels.road.tollbooth.app;

import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.tollbooth.client.api.PatchSet;

@Slf4j
@Component
@RequiredArgsConstructor
public class TollBooth implements ApplicationRunner, AutoCloseable {
  private final ObjectMapper mapper;
  private final Consumer<String, String> patchConsumer;
  private final PatchProcessor patchProcessor;
  private final Disposable.Swap disposabe = Disposables.swap();

  @Override
  public void run(ApplicationArguments args) throws Exception {
    disposabe.update(Mono
        .fromSupplier(() -> patchConsumer.poll(100))
        .repeat()
        .flatMapIterable(Function.identity())
        .map(ConsumerRecord::value)
        .subscribeOn(Schedulers.single())
        .subscribe(this::processPatchSet));
  }

  @Override
  public void close() throws Exception {
    disposabe.dispose();
  }

  private void processPatchSet(String patchSetJson) {
    try {
      log.info(patchSetJson);
      PatchSet patchSet = mapper.readValue(patchSetJson, PatchSet.class);
      patchProcessor.processPatch(patchSet);
    } catch (Exception e) {
      log.error("Error applying patch to document: {}", patchSetJson, e);
    }
  }
}
