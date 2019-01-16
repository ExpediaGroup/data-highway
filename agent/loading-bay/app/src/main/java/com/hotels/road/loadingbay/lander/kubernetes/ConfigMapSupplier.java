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

import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import lombok.extern.slf4j.Slf4j;

import com.hotels.road.loadingbay.lander.TruckParkConstants;

@Slf4j
@Component
public class ConfigMapSupplier implements Supplier<ConfigMap>, Watcher<ConfigMap>, AutoCloseable {

  private final Object lock = new Object();

  private final String name;
  private ConfigMap configMap;
  private final Watch watch;

  @Autowired
  public ConfigMapSupplier(KubernetesClient client, @Value(TruckParkConstants.TRUCK_PARK) String name) {
    this.name = name;
    Resource<ConfigMap, DoneableConfigMap> resource = client.configMaps().withName(name);
    configMap = resource.get();
    if (configMap == null) {
      throw new RuntimeException(String.format("ConfigMap %s does not exist.", name));
    }
    watch = resource.watch(this);
  }

  @Override
  public ConfigMap get() {
    synchronized (lock) {
      if (configMap == null) {
        throw new IllegalStateException(String.format("ConfigMap %s has been deleted!", name));
      }
      return configMap;
    }
  }

  @Override
  public void eventReceived(Action action, ConfigMap configMap) {
    synchronized (lock) {
      switch (action) {
      case MODIFIED:
        log.info("ConfigMap {} was modified. {}", name, configMap);
        this.configMap = configMap;
        break;
      case ADDED:
        log.warn("ConfigMap {} should never be ADDED during lifetime of this instance.", name);
        this.configMap = configMap;
        break;
      case DELETED:
        log.warn("ConfigMap {} should never be DELETED during lifetime of this instance.", name);
        this.configMap = null;
        break;
      case ERROR:
        throw new RuntimeException(String.format("An unknown error occurred with ConfigMap %s.", name));
      default:
        throw new RuntimeException("Unknown Action:" + action.name());
      }
    }
  }

  @Override
  public void onClose(KubernetesClientException cause) {
    if (cause != null) {
      log.warn("An error occurred while watching the ConfigMap.", cause);
    }
    log.info("Closing...");
  }

  @Override
  public void close() throws Exception {
    watch.close();
  }

}
