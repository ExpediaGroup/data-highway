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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

@Configuration
public class KubernetesConfiguration {

  @Bean
  public KubernetesClient kubernetesClient(
      @Value("${kubernetes.masterUrl:https://kubernetes.default.svc:443}") String masterUrl,
      @Value("${kubernetes.namespace}") String namespace) {
    return new DefaultKubernetesClient(new ConfigBuilder().withMasterUrl(masterUrl).withNamespace(namespace).build());
  }

}
