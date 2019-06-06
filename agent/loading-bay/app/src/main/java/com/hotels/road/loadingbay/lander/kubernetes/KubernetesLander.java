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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Component;

import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.PodResource;
import lombok.RequiredArgsConstructor;

import com.hotels.road.loadingbay.lander.Lander;
import com.hotels.road.loadingbay.lander.LanderConfiguration;

@RequiredArgsConstructor
public class KubernetesLander implements Lander {
  private final LanderConfiguration config;
  private final Pod pod;
  private final String podName;
  private final KubernetesClient client;

  @Override
  public CompletableFuture<LanderConfiguration> run() {
    client.pods().withName(podName).delete();
    client.pods().create(pod);
    PodResource<Pod, DoneablePod> resource = client.pods().withName(podName);
    PodWatcher podWatcher = new PodWatcher(resource, podName);
    return podWatcher.get().thenApply(c -> config);
  }

  @Component("kubernetesLanderFactory")
  @RequiredArgsConstructor
  public static class Factory implements Lander.Factory {
    private final ArgsFactory argsFactory;
    private final LanderPodFactory podFactory;
    private final PodNameFactory podNameFactory;
    private final KubernetesClient client;

    @Override
    public Lander newInstance(LanderConfiguration config) {
      List<String> args = argsFactory.newInstance(config);
      Pod pod = podFactory.newInstance(config, args);
      String podName = podNameFactory.newName(config);
      return new KubernetesLander(config, pod, podName, client);
    }
  }
}
