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

import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.fabric8.kubernetes.api.model.ConfigMap;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.loadingbay.lander.LanderConfiguration;
import com.hotels.road.loadingbay.lander.OffsetRange;

@Slf4j
@Component
public class ArgsFactory {

  private final String bootstrapServers;
  private final String roadModelTopic;
  private final String piiReplacerClassName;
  private final String s3Bucket;
  private final String s3EndpointUrl;
  private final String s3EndpointSigningRegion;
  private final String graphiteEndpoint;
  private final Supplier<ConfigMap> configMapSupplier;

  @Autowired
  ArgsFactory(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.road.topic}") String roadModelTopic,
      @Value("${piiReplacerClassName}") String piiReplacerClassName,
      @Value("${hive.table.location.bucket}") String s3Bucket,
      @Value("${s3.endpoint.url}") String s3EndpointUrl,
      @Value("${s3.endpoint.signingRegion}") String s3EndpointSigningRegion,
      @Value("${graphite.endpoint:disabled}") String graphiteEndpoint,
      Supplier<ConfigMap> configMapSupplier) {
    this.bootstrapServers = bootstrapServers;
    this.roadModelTopic = roadModelTopic;
    this.piiReplacerClassName = piiReplacerClassName;
    this.s3Bucket = s3Bucket;
    this.s3EndpointUrl = s3EndpointUrl;
    this.s3EndpointSigningRegion = s3EndpointSigningRegion;
    this.graphiteEndpoint = graphiteEndpoint;
    this.configMapSupplier = configMapSupplier;
  }

  public List<String> newInstance(LanderConfiguration config) {
    String formattedOffsets = formatOffsets(config.getOffsets());
    log.info("Formatted offsets for topic: {} to launch Truck Park with: {}", config.getTopicName(), formattedOffsets);
    return ImmutableMap
        .<String, String> builder()
        .put("spring.profiles.active", configMapSupplier.get().getData().get("profiles"))
        .put("kafka.bootstrapServers", bootstrapServers)
        .put("road.name", config.getRoadName())
        .put("road.topic", config.getTopicName())
        .put("kafka.road.topic", roadModelTopic)
        .put("piiReplacerClassName", piiReplacerClassName)
        .put("road.offsets", formattedOffsets)
        .put("s3.bucket", s3Bucket)
        .put("s3.keyPrefix", config.getS3KeyPrefix())
        .put("s3.endpoint.url", s3EndpointUrl)
        .put("s3.endpoint.signingRegion", s3EndpointSigningRegion)
        .put("s3.enableServerSideEncryption", Boolean.toString(config.isEnableServerSideEncryption()))
        .put("metrics.graphiteEndpoint", graphiteEndpoint)
        .build()
        .entrySet()
        .stream()
        .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }

  private String formatOffsets(Map<Integer, OffsetRange> offsets) {
    return offsets.entrySet().stream().map(this::format).collect(joining(";"));
  }

  private String format(Entry<Integer, OffsetRange> entry) {
    Integer partition = entry.getKey();
    OffsetRange offsetRange = entry.getValue();
    return partition + ":" + offsetRange.getStart() + "," + offsetRange.getEnd();
  }

}
