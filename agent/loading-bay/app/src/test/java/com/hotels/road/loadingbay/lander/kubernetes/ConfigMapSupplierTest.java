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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

@RunWith(MockitoJUnitRunner.class)
public class ConfigMapSupplierTest {

  @Mock
  private MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> configMaps;
  @Mock
  private Resource<ConfigMap, DoneableConfigMap> resource;
  @Mock
  private Watch watch;
  @Mock
  private ConfigMap configMap1;
  @Mock
  private ConfigMap configMap2;
  @Mock
  private KubernetesClient client;

  private final String name = "name";

  @SuppressWarnings("unchecked")
  @Before
  public void before() {
    when(client.configMaps()).thenReturn(configMaps);
    when(configMaps.withName(name)).thenReturn(resource);
    when(resource.watch(any(Watcher.class))).thenReturn(watch);
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenConfigMapDoesNotExist() throws Exception {
    try (ConfigMapSupplier underTest = new ConfigMapSupplier(client, name)) {}
  }

  @Test
  public void get() throws Exception {
    when(resource.get()).thenReturn(configMap1);
    try (ConfigMapSupplier underTest = new ConfigMapSupplier(client, name)) {
      assertThat(underTest.get(), is(configMap1));
    }
    verify(watch).close();
  }

  @Test
  public void added() throws Exception {
    when(resource.get()).thenReturn(configMap1);
    try (ConfigMapSupplier underTest = new ConfigMapSupplier(client, name)) {
      assertThat(underTest.get(), is(configMap1));
      underTest.eventReceived(Action.ADDED, configMap2);
      assertThat(underTest.get(), is(configMap2));
    }
  }

  @Test
  public void modified() throws Exception {
    when(resource.get()).thenReturn(configMap1);
    try (ConfigMapSupplier underTest = new ConfigMapSupplier(client, name)) {
      assertThat(underTest.get(), is(configMap1));
      underTest.eventReceived(Action.MODIFIED, configMap2);
      assertThat(underTest.get(), is(configMap2));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void deleted() throws Exception {
    when(resource.get()).thenReturn(configMap1);
    try (ConfigMapSupplier underTest = new ConfigMapSupplier(client, name)) {
      underTest.eventReceived(Action.DELETED, null);
      underTest.get();
    }
  }

  @Test(expected = RuntimeException.class)
  public void error() throws Exception {
    when(resource.get()).thenReturn(configMap1);
    try (ConfigMapSupplier underTest = new ConfigMapSupplier(client, name)) {
      underTest.eventReceived(Action.ERROR, null);
    }
  }

}
