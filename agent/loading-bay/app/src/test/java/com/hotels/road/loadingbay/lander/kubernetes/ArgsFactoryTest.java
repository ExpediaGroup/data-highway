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
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.fabric8.kubernetes.api.model.ConfigMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.hotels.road.loadingbay.lander.LanderConfiguration;
import com.hotels.road.loadingbay.lander.OffsetRange;

@RunWith(MockitoJUnitRunner.class)
public class ArgsFactoryTest {

  @Mock
  private Supplier<ConfigMap> configMapSupplier;
  @Mock
  private ConfigMap configMap;

  private ArgsFactory underTest;

  @Test
  public void typical() {
    when(configMapSupplier.get()).thenReturn(configMap);
    when(configMap.getData()).thenReturn(ImmutableMap.of("profiles", "default"));
    underTest = new ArgsFactory("bootstrapServers", "kafkaStoreTopic", "piiReplacerClassName", "s3Bucket",
        "s3EndpointUrl", "s3EndpointSigningRegion", "graphiteEndpoint", configMapSupplier);

    HashMap<Integer, OffsetRange> roadOffsets = Maps.newHashMap();
    roadOffsets.put(1, new OffsetRange(1, 2));
    roadOffsets.put(2, new OffsetRange(2, 3));
    roadOffsets.put(3, new OffsetRange(3, 4));
    LanderConfiguration landerConfiguration = new LanderConfiguration("name", "topic", roadOffsets, "s3KeyPrefix",
        false, "partitionColumnValue", false);
    List<String> result = underTest.newInstance(landerConfiguration);

    List<String> expected = ImmutableList
        .<String> builder()
        .add("--spring.profiles.active=default")
        .add("--kafka.bootstrapServers=bootstrapServers")
        .add("--road.name=name")
        .add("--road.topic=topic")
        .add("--kafka.road.topic=kafkaStoreTopic")
        .add("--piiReplacerClassName=piiReplacerClassName")
        .add("--road.offsets=1:1,2;2:2,3;3:3,4")
        .add("--s3.bucket=s3Bucket")
        .add("--s3.keyPrefix=s3KeyPrefix")
        .add("--s3.endpoint.url=s3EndpointUrl")
        .add("--s3.endpoint.signingRegion=s3EndpointSigningRegion")
        .add("--s3.enableServerSideEncryption=false")
        .add("--metrics.graphiteEndpoint=graphiteEndpoint")
        .build();

    assertThat(result, is(expected));
  }
}
