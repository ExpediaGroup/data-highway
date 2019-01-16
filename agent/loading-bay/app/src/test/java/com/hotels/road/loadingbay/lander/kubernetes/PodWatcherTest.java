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
package com.hotels.road.loadingbay.lander.kubernetes;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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

@RunWith(MockitoJUnitRunner.class)
public class PodWatcherTest {

  private static final String POD_NAME = "test_pod";

  @Mock
  private PodResource<Pod, DoneablePod> resource;
  @Mock
  private Watch watch;
  @Mock
  private Pod pod;
  @Mock
  private PodStatus podStatus;
  @Mock
  private ContainerStatus containerStatus;
  @Mock
  private ContainerState containerState;
  @Mock
  private ContainerStateTerminated containerStateTerminated;

  private PodWatcher underTest;

  @SuppressWarnings("unchecked")
  @Test
  public void normalOperation() {
    when(resource.get()).thenReturn(pod);
    when(resource.watch(any(Watcher.class))).thenReturn(watch);
    underTest = new PodWatcher(resource, POD_NAME);

    when(pod.getStatus()).thenReturn(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(singletonList(containerStatus));
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getExitCode()).thenReturn(0);

    underTest.eventReceived(null, pod);
    CompletableFuture<Integer> result = underTest.get();
    underTest.onClose(null);

    assertThat(result.isDone(), is(true));
    verify(resource).delete();
    verify(watch).close();
    assertThat(result.join(), is(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void eventReceived_containerStatusesIsEmpty() {
    when(resource.get()).thenReturn(pod);
    when(resource.watch(any(Watcher.class))).thenReturn(watch);
    underTest = new PodWatcher(resource, POD_NAME);

    when(pod.getStatus()).thenReturn(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(emptyList());

    underTest.eventReceived(null, pod);
    CompletableFuture<Integer> result = underTest.get();

    assertThat(result.isDone(), is(false));
    verify(resource, never()).delete();
    verify(watch, never()).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void eventReceived_nullTerminated() {
    when(resource.get()).thenReturn(pod);
    when(resource.watch(any(Watcher.class))).thenReturn(watch);
    underTest = new PodWatcher(resource, POD_NAME);

    when(pod.getStatus()).thenReturn(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(singletonList(containerStatus));
    when(containerStatus.getState()).thenReturn(containerState);

    underTest.eventReceived(null, pod);
    CompletableFuture<Integer> result = underTest.get();

    assertThat(result.isDone(), is(false));
    verify(resource, never()).delete();
    verify(watch, never()).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void eventReceived_nonZeroExitCode() {
    when(resource.get()).thenReturn(pod);
    when(resource.watch(any(Watcher.class))).thenReturn(watch);
    underTest = new PodWatcher(resource, POD_NAME);

    when(pod.getStatus()).thenReturn(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(singletonList(containerStatus));
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getExitCode()).thenReturn(1);

    underTest.eventReceived(null, pod);
    CompletableFuture<Integer> result = underTest.get();

    assertThat(result.isDone(), is(true));
    verify(resource).delete();
    verify(watch).close();
    try {
      result.join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), is(instanceOf(RuntimeException.class)));
    }
  }

  @Test(expected = RuntimeException.class)
  public void noPodExists_throwsRuntimeException() {
    new PodWatcher(resource, POD_NAME);
  }

  @Test
  public void closeNotComplete() {
    when(resource.get()).thenReturn(pod);
    underTest = new PodWatcher(resource, POD_NAME);

    underTest.onClose(null);
    CompletableFuture<Integer> result = underTest.get();

    assertThat(result.isDone(), is(true));
    try {
      result.join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), is(instanceOf(IllegalStateException.class)));
    }
  }

  @Test
  public void cancel_deletesResource() throws Exception {
    when(resource.get()).thenReturn(pod);
    underTest = new PodWatcher(resource, POD_NAME);

    underTest.get().cancel(true);

    verify(resource).delete();
  }

  @Test
  public void closeExceptionally() {
    when(resource.get()).thenReturn(pod);
    underTest = new PodWatcher(resource, POD_NAME);

    underTest.onClose(new KubernetesClientException("message"));
    CompletableFuture<Integer> result = underTest.get();

    assertThat(result.isDone(), is(true));
    try {
      result.join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), is(instanceOf(KubernetesClientException.class)));
    }
  }

}
