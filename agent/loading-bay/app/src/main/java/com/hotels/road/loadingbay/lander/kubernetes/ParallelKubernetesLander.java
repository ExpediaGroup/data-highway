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
package com.hotels.road.loadingbay.lander.kubernetes;

import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.google.common.collect.Lists;

import com.hotels.road.loadingbay.lander.Lander;
import com.hotels.road.loadingbay.lander.LanderConfiguration;

@RequiredArgsConstructor
public class ParallelKubernetesLander implements Lander {
  private final LanderConfiguration landerConfig;
  private final List<Lander> landers;

  @Override
  public CompletableFuture<LanderConfiguration> run() {
    CompletableFuture<?> anyException = new CompletableFuture<>();

    List<CompletableFuture<LanderConfiguration>> landerFutures = landers.stream().map(Lander::run).collect(toList());

    landerFutures.forEach(f -> f.exceptionally(t -> {
      landerFutures.forEach(lander -> lander.cancel(true));
      anyException.completeExceptionally(t);
      return null;
    }));

    CompletableFuture<Void> allSucceed = runAsync(() -> landerFutures.forEach(CompletableFuture::join));

    return anyOf(anyException, allSucceed).thenApply(x -> landerConfig);
  }

  @Component("landerFactory")
  public static class Factory implements Lander.Factory {
    private final Lander.Factory delegate;
    private final int partitionsPerPod;

    public Factory(
        @Value("#{kubernetesLanderFactory}") Lander.Factory delegate,
        @Value("${partitionsPerPod:6}") int partitionsPerPod) {
      this.delegate = delegate;
      this.partitionsPerPod = partitionsPerPod;
    }

    @Override
    public Lander newInstance(LanderConfiguration landerConfig) {
      List<Lander> landers = Stream
          .of(landerConfig.getOffsets())
          .map(Map::keySet)
          .map(ArrayList::new)
          .map(l -> Lists.partition(l, partitionsPerPod))
          .flatMap(List::stream)
          .map(x -> x.stream().collect(Collectors.toMap(identity(), p -> landerConfig.getOffsets().get(p))))
          .map(landerConfig::withOffsets)
          .map(delegate::newInstance)
          .collect(toList());
      return new ParallelKubernetesLander(landerConfig, landers);
    }
  }
}
