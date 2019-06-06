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
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.PodResource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PodWatcher implements Supplier<CompletableFuture<Integer>>, Watcher<Pod> {
  private final PodResource<Pod, DoneablePod> resource;
  private final String podName;
  private final Watch watch;
  private final CompletableFuture<Integer> exitCodeFuture;

  public PodWatcher(PodResource<Pod, DoneablePod> resource, String podName) {
    this.resource = resource;
    this.podName = podName;
    if (resource.get() == null) {
      throw new RuntimeException(String.format("pod %s does not exist.", podName));
    }
    watch = resource.watch(this);
    exitCodeFuture = new CompletableFuture<Integer>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        resource.delete();
        return super.cancel(mayInterruptIfRunning);
      }
    };
  }

  @Override
  public void onClose(KubernetesClientException cause) {
    if (!exitCodeFuture.isDone()) {
      if (cause == null) {
        exitCodeFuture
            .completeExceptionally(new IllegalStateException("Watcher closed normally without completing the future."));
      } else {
        exitCodeFuture.completeExceptionally(cause);
      }
    }
  }

  @Override
  public CompletableFuture<Integer> get() {
    return exitCodeFuture;
  }

  @Override
  public void eventReceived(io.fabric8.kubernetes.client.Watcher.Action action, Pod pod) {
    log.info("Event received for pod: {}, action: {}", podName, action);
    PodStatus status = pod.getStatus();
    List<ContainerStatus> containerStatuses = status.getContainerStatuses();
    if (!containerStatuses.isEmpty()) {
      ContainerStatus containerStatus = containerStatuses.get(0);
      ContainerState state = containerStatus.getState();
      ContainerStateTerminated terminated = state.getTerminated();
      if (terminated != null) {
        Integer exitCode = terminated.getExitCode();
        log.info("Container exit code for pod {}: {}", podName, exitCode);
        if (exitCode == 0) {
          exitCodeFuture.complete(0);
        } else {
          exitCodeFuture.completeExceptionally(new RuntimeException("Completed with non zero exit code: " + exitCode));
        }
        resource.delete();
        watch.close();
      } else {
        log.warn("ContainerStateTerminated was null for pod: {}, action {}", podName, action);
      }
    } else {
      log.warn("ContainerStatus list was empty for pod: {}, action {}", podName, action);
    }
  }

}
