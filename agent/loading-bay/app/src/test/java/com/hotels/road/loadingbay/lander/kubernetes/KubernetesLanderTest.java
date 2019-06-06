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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.loadingbay.lander.Lander;
import com.hotels.road.loadingbay.lander.LanderConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesLanderTest {

  private static final List<String> ARGS = Arrays.asList("a");
  @Mock
  private LanderPodFactory podFactory;
  @Mock
  private ArgsFactory argsFactory;
  @Mock
  private KubernetesClient client;
  @Mock
  private MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mixedOperation;
  @Mock
  private PodResource<Pod, DoneablePod> podResource;
  @Mock
  private Watch watch;
  @Mock
  private PodNameFactory podNameFactory;
  @Captor
  private ArgumentCaptor<PodWatcher> podWatcherCaptor;

  @Test
  public void successfulPodCompletion() throws Exception {
    when(podNameFactory.newName(any(LanderConfiguration.class))).thenReturn("pod-name");
    when(podResource.watch(podWatcherCaptor.capture())).thenReturn(watch);
    when(client.pods()).thenReturn(mixedOperation);
    when(mixedOperation.withName(anyString())).thenReturn(podResource);
    Pod pod = new Pod();
    when(podResource.get()).thenReturn(pod);
    KubernetesLander.Factory factory = new KubernetesLander.Factory(argsFactory, podFactory, podNameFactory, client);
    LanderConfiguration config = new LanderConfiguration("road", "topic", ImmutableMap.of(), "s3Prefix", false,
        "partitionColumnValue", false);
    when(argsFactory.newInstance(config)).thenReturn(ARGS);
    when(podFactory.newInstance(config, ARGS)).thenReturn(pod);
    Lander lander = factory.newInstance(config);
    CompletableFuture<LanderConfiguration> future = lander.run();
    verify(podResource).delete();
    verify(mixedOperation).create(pod);

    pod.setStatus(terminatedPostStatus(0));
    podWatcherCaptor.getValue().eventReceived(null, pod); // trigger successful pod termination.
    assertThat(config, is(future.get()));
  }

  @Test
  public void nonZeroExitCode() throws Exception {
    when(podNameFactory.newName(any(LanderConfiguration.class))).thenReturn("pod-name");
    when(podResource.watch(podWatcherCaptor.capture())).thenReturn(watch);
    when(client.pods()).thenReturn(mixedOperation);
    when(mixedOperation.withName(anyString())).thenReturn(podResource);
    Pod pod = new Pod();
    when(podResource.get()).thenReturn(pod);
    KubernetesLander.Factory factory = new KubernetesLander.Factory(argsFactory, podFactory, podNameFactory, client);
    LanderConfiguration config = new LanderConfiguration("road", "topic", ImmutableMap.of(), "s3Prefix", false,
        "partitionColumnValue", false);
    when(argsFactory.newInstance(config)).thenReturn(ARGS);
    when(podFactory.newInstance(config, ARGS)).thenReturn(pod);
    Lander lander = factory.newInstance(config);
    CompletableFuture<LanderConfiguration> run = lander.run();
    verify(podResource).delete();
    verify(mixedOperation).create(pod);

    pod.setStatus(terminatedPostStatus(1));
    podWatcherCaptor.getValue().eventReceived(null, pod);
    assertThat(run.isCompletedExceptionally(), is(true));
  }

  private PodStatus terminatedPostStatus(int exitCode) {
    ContainerStatus containerStatus = new ContainerStatus();
    containerStatus.setState(terminatedStateWithExitCode(exitCode));
    PodStatus podStatus = new PodStatus();
    podStatus.setContainerStatuses(Collections.singletonList(containerStatus));
    return podStatus;
  }

  private ContainerState terminatedStateWithExitCode(int exitCode) {
    ContainerStateTerminated terminated = new ContainerStateTerminated();
    terminated.setExitCode(exitCode);
    ContainerState state = new ContainerState();
    state.setTerminated(terminated);
    return state;
  }
}
