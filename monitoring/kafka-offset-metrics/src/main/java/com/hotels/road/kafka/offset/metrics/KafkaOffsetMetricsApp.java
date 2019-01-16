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
package com.hotels.road.kafka.offset.metrics;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import kafka.admin.AdminClient;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.net.HostAndPort;

import com.hotels.road.boot.DataHighwayApplication;

@SpringBootApplication
@EnableScheduling
public class KafkaOffsetMetricsApp {
  @Bean
  AdminClient adminClient(@Value("${kafka.bootstrapServers}") String bootstrapServers) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", bootstrapServers);
    return AdminClient.create(properties);
  }

  @Bean
  ScheduledReporter reporter(
      @Value("${graphite.endpoint}") String graphiteEndpoint,
      @Value("${graphite.prefix:road}") String graphitePrefix,
      Clock clock,
      Supplier<String> hostnameSupplier) {
    HostAndPort hostAndPort = HostAndPort.fromString(graphiteEndpoint);
    InetSocketAddress socketAddress = new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
    return GraphiteReporter
        .forRegistry(new MetricRegistry())
        .prefixedWith(MetricRegistry.name(graphitePrefix, "kafka-offset", "host", hostnameSupplier.get()))
        .withClock(clock)
        .build(new Graphite(socketAddress));
  }

  @Bean
  Clock clock() {
    return Clock.defaultClock();
  }

  @Bean
  Supplier<String> hostnameSupplier() {
    return () -> {
      try {
        return InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static void main(String[] args) {
    DataHighwayApplication.run(KafkaOffsetMetricsApp.class, args);
  }
}
