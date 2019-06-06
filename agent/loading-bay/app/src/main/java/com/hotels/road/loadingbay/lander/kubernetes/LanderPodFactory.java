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

import static com.hotels.road.loadingbay.lander.TruckParkConstants.TRUCK_PARK;
import static com.hotels.road.loadingbay.lander.kubernetes.Labels.APP;
import static com.hotels.road.loadingbay.lander.kubernetes.Labels.ROAD;
import static com.hotels.road.loadingbay.lander.kubernetes.Labels.VERSION;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.HTTPGetActionBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectFieldSelector;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.loadingbay.lander.LanderConfiguration;

@Component
@Slf4j
public class LanderPodFactory {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<Map<String, String>> MAP_STRING_STRING = new TypeReference<Map<String, String>>() {};
  public static final String DOCKER_IMAGE = "image_name";
  static final String RESTART_POLICY_NEVER = "Never";
  static final String CPU = "cpu";
  static final String MEMORY = "memory";
  static final String JVM_ARGS = "jvmArgs";
  static final String ANNOTATIONS = "annotations";
  static final String CLOUDWATCH_REGION = "cloudwatchRegion";
  static final String CLOUDWATCH_GROUP = "cloudwatchGroup";

  private final Supplier<ConfigMap> configMapSupplier;
  private final PodNameFactory podNameFactory;
  private final String environment;

  @Autowired
  public LanderPodFactory(
      Supplier<ConfigMap> configMapSupplier,
      PodNameFactory podNameFactory,
      @Value("${ENVIRONMENT}") String environment) {
    this.configMapSupplier = configMapSupplier;
    this.podNameFactory = podNameFactory;
    this.environment = environment;
  }

  public Pod newInstance(LanderConfiguration config, List<String> args) {
    String truckParkName = podNameFactory.newName(config);
    log.info("Creating pod named: {}", truckParkName);
    Map<String, String> configMap = configMapSupplier.get().getData();
    return new PodBuilder()
        .withMetadata(new ObjectMetaBuilder()
            .withName(truckParkName)
            .withLabels(labels(config.getRoadName(), configMap))
            .withAnnotations(annotations(configMap))
            .build())
        .withSpec(new PodSpecBuilder()
            .withRestartPolicy(RESTART_POLICY_NEVER)
            .withContainers(container(config.getRoadName(), args, configMap, truckParkName))
            .build())
        .build();
  }

  private Map<String, String> annotations(Map<String, String> configMap) {
    try {
      return mapper.readValue(configMap.getOrDefault(ANNOTATIONS, "{}"), MAP_STRING_STRING);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Container container(String roadName, List<String> args, Map<String, String> config, String truckParkName) {
    List<EnvVar> env = ImmutableList
        .<EnvVar> builder()
        .add(envFromFieldPath("KUBERNETES_NAMESPACE", "metadata.namespace"))
        .add(env("POD_NAME", truckParkName))
        .add(env("ENVIRONMENT", environment))
        .add(env("JVM_ARGS", config.get(JVM_ARGS)))
        .add(env("CLOUDWATCH_REGION", config.get(CLOUDWATCH_REGION)))
        .add(env("CLOUDWATCH_GROUP", config.get(CLOUDWATCH_GROUP)))
        .add(env("CLOUDWATCH_STREAM", "${KUBERNETES_NAMESPACE}-truck-park-" + roadName))
        .build();
    Map<String, Quantity> limits = ImmutableMap
        .<String, Quantity> builder()
        .put(CPU, new Quantity(config.get(CPU)))
        .put(MEMORY, new Quantity(config.get(MEMORY)))
        .build();
    return new ContainerBuilder()
        .withName(truckParkName)
        .withImage(config.get(DOCKER_IMAGE))
        .withArgs(args)
        .withEnv(env)
        .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(limits).build())
        .withLivenessProbe(new ProbeBuilder()
            .withHttpGet(new HTTPGetActionBuilder().withPath("/").withPort(new IntOrString("http")).build())
            .withInitialDelaySeconds(getConfigOrDefault(config, "livenessInitialDelay", 30))
            .withPeriodSeconds(getConfigOrDefault(config, "livenessPeriod", 5))
            .withSuccessThreshold(getConfigOrDefault(config, "livenessSuccessThreshold", 1))
            .withTimeoutSeconds(getConfigOrDefault(config, "livenessTimeout", 5))
            .withFailureThreshold(getConfigOrDefault(config, "livenessFailureThreshold", 3))
            .build())
        .build();
  }

  private Integer getConfigOrDefault(Map<String, String> config, String key, int defaultValue) {
    String value = config.get(key);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return defaultValue;
  }

  private EnvVar env(String name, String value) {
    return new EnvVarBuilder().withName(name).withValue(value).build();
  }

  private EnvVar envFromFieldPath(String name, String fieldPath) {
    EnvVarSource envVarSource = new EnvVarSource();
    envVarSource.setFieldRef(new ObjectFieldSelector());
    ObjectFieldSelector fieldRef = envVarSource.getFieldRef();
    fieldRef.setFieldPath(fieldPath);
    return new EnvVarBuilder().withName(name).withValueFrom(envVarSource).build();
  }

  private Map<String, String> labels(String roadName, Map<String, String> config) {
    return ImmutableMap
        .<String, String> builder()
        .put(APP, TRUCK_PARK)
        .put(VERSION, config.get(VERSION))
        .put(ROAD, roadName)
        .build();
  }

}
