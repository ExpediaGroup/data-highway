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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import static com.hotels.road.loadingbay.lander.TruckParkConstants.TRUCK_PARK;
import static com.hotels.road.loadingbay.lander.kubernetes.Labels.APP;
import static com.hotels.road.loadingbay.lander.kubernetes.Labels.ROAD;
import static com.hotels.road.loadingbay.lander.kubernetes.Labels.VERSION;
import static com.hotels.road.loadingbay.lander.kubernetes.LanderPodFactory.CLOUDWATCH_GROUP;
import static com.hotels.road.loadingbay.lander.kubernetes.LanderPodFactory.CLOUDWATCH_REGION;
import static com.hotels.road.loadingbay.lander.kubernetes.LanderPodFactory.CPU;
import static com.hotels.road.loadingbay.lander.kubernetes.LanderPodFactory.DOCKER_IMAGE;
import static com.hotels.road.loadingbay.lander.kubernetes.LanderPodFactory.JVM_ARGS;
import static com.hotels.road.loadingbay.lander.kubernetes.LanderPodFactory.MEMORY;
import static com.hotels.road.loadingbay.lander.kubernetes.LanderPodFactory.RESTART_POLICY_NEVER;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.loadingbay.lander.LanderConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class LanderPodFactoryTest {

  private static final String NAME = "the_name";
  private static final String TRUCK_PARK_NAME = "truck-park-the-name";
  private static final String ARG = "--foo=bar";
  private static final String DOCKER_IMAGE_NAME = "image-name";
  private static final String IMAGE_VERSION = "0.1.2";
  private static final String PARTITION_COLUMN_VALUE = "38473838";

  @Mock
  private Supplier<ConfigMap> configMapSupplier;
  @Mock
  private ConfigMap configMap;
  @Mock
  private PodNameFactory podNameFactory;

  private LanderPodFactory underTest;

  @Before
  public void before() {
    underTest = new LanderPodFactory(configMapSupplier, podNameFactory, "lab");
  }

  @Test
  public void simple() {
    when(podNameFactory.newName(any())).thenReturn(TRUCK_PARK_NAME);
    when(configMapSupplier.get()).thenReturn(configMap);
    when(configMap.getData()).thenReturn(ImmutableMap
        .<String, String> builder()
        .put(DOCKER_IMAGE, DOCKER_IMAGE_NAME)
        .put(VERSION, IMAGE_VERSION)
        .put(CPU, "cpu1")
        .put(MEMORY, "mem1")
        .put(JVM_ARGS, "args1")
        .put(CLOUDWATCH_REGION, "region")
        .put(CLOUDWATCH_GROUP, "group")
        .put("annotations", "{\"annotation1\": \"value1\", \"annotation2/slashsomething\": \"value2\"}")
        .build());

    LanderConfiguration config = new LanderConfiguration(NAME, "topic", emptyMap(), "s3Prefix", false,
        PARTITION_COLUMN_VALUE, false);
    Pod pod = underTest.newInstance(config, singletonList(ARG));

    ObjectMeta metadata = pod.getMetadata();
    assertThat(metadata.getName(), is(TRUCK_PARK_NAME));
    assertThat(metadata.getLabels(),
        is(ImmutableMap
            .<String, String> builder()
            .put(APP, TRUCK_PARK)
            .put(VERSION, IMAGE_VERSION)
            .put(ROAD, NAME)
            .build()));
    assertThat(metadata.getAnnotations(),
        is(ImmutableMap
            .<String, String> builder()
            .put("annotation1", "value1")
            .put("annotation2/slashsomething", "value2")
            .build()));

    PodSpec spec = pod.getSpec();
    assertThat(spec.getRestartPolicy(), is(RESTART_POLICY_NEVER));

    List<Container> containers = spec.getContainers();
    assertThat(containers.size(), is(1));

    Container container = containers.get(0);
    assertThat(container.getName(), is(TRUCK_PARK_NAME));
    assertThat(container.getImage(), is(DOCKER_IMAGE_NAME));
    assertThat(container.getArgs(), is(singletonList(ARG)));

    List<EnvVar> env = container.getEnv();
    assertThat(env.size(), is(7));

    EnvVar kubeNamespace = env.get(0);
    assertThat(kubeNamespace.getName(), is("KUBERNETES_NAMESPACE"));
    assertThat(kubeNamespace.getValue(), is(nullValue()));

    EnvVar podNameEnv = env.get(1);
    assertThat(podNameEnv.getName(), is("POD_NAME"));
    assertThat(podNameEnv.getValue(), is(TRUCK_PARK_NAME));

    EnvVar environment = env.get(2);
    assertThat(environment.getName(), is("ENVIRONMENT"));
    assertThat(environment.getValue(), is("lab"));

    EnvVar jvmArgs = env.get(3);
    assertThat(jvmArgs.getName(), is("JVM_ARGS"));
    assertThat(jvmArgs.getValue(), is("args1"));

    EnvVar cloudwatchRegion = env.get(4);
    assertThat(cloudwatchRegion.getName(), is("CLOUDWATCH_REGION"));
    assertThat(cloudwatchRegion.getValue(), is("region"));

    EnvVar cloudwatchGroup = env.get(5);
    assertThat(cloudwatchGroup.getName(), is("CLOUDWATCH_GROUP"));
    assertThat(cloudwatchGroup.getValue(), is("group"));

    EnvVar cloudwatchStream = env.get(6);
    assertThat(cloudwatchStream.getName(), is("CLOUDWATCH_STREAM"));
    assertThat(cloudwatchStream.getValue(), is("${KUBERNETES_NAMESPACE}-truck-park-the_name"));

    ResourceRequirements resources = container.getResources();
    assertThat(resources.getLimits().get(CPU), is(new Quantity("cpu1")));
    assertThat(resources.getLimits().get(MEMORY), is(new Quantity("mem1")));
    assertThat(resources.getRequests().get(CPU), is(new Quantity("cpu1")));
    assertThat(resources.getRequests().get(MEMORY), is(new Quantity("mem1")));
  }

  @Test
  public void podBuildingWithoutAnnotations() {
    when(podNameFactory.newName(any())).thenReturn(TRUCK_PARK_NAME);
    when(configMapSupplier.get()).thenReturn(configMap);
    when(configMap.getData()).thenReturn(ImmutableMap
            .<String, String> builder()
            .put(DOCKER_IMAGE, DOCKER_IMAGE_NAME)
            .put(VERSION, IMAGE_VERSION)
            .put(CPU, "cpu1")
            .put(MEMORY, "mem1")
            .put(JVM_ARGS, "args1")
            .put(CLOUDWATCH_REGION, "region")
            .put(CLOUDWATCH_GROUP, "group")
            .build());

    LanderConfiguration config = new LanderConfiguration(NAME, "topic", emptyMap(), "s3Prefix", false,
            PARTITION_COLUMN_VALUE, false);
    Pod pod = underTest.newInstance(config, singletonList(ARG));

    ObjectMeta metadata = pod.getMetadata();
    assertThat(metadata.getName(), is(TRUCK_PARK_NAME));
    assertThat(metadata.getLabels(),
            is(ImmutableMap
                    .<String, String> builder()
                    .put(APP, TRUCK_PARK)
                    .put(VERSION, IMAGE_VERSION)
                    .put(ROAD, NAME)
                    .build()));
    assertThat(metadata.getAnnotations(),
            is(Collections.emptyMap()));
  }
}
