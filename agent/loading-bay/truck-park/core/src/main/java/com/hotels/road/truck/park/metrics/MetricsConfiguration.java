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
package com.hotels.road.truck.park.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.net.HostAndPort;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;

@Configuration
@EnableMetrics(proxyTargetClass = true)
@Slf4j
public class MetricsConfiguration extends MetricsConfigurerAdapter {
  private final InetSocketAddress graphiteEndpoint;
  private final String roadName;
  private GraphiteReporter reporter;

  @Autowired
  public MetricsConfiguration(
      @Value("${metrics.graphiteEndpoint:disabled}") String graphiteEndpoint,
      @Value("${road.name}") String roadName) {
    if ("disabled".equalsIgnoreCase(graphiteEndpoint)) {
      log.info("Graphite metrics reporting is disabled");
      this.graphiteEndpoint = null;
    } else {
      log.info("Graphite reporting is configured for {}", graphiteEndpoint);
      HostAndPort hostAndPort = HostAndPort.fromString(graphiteEndpoint);
      this.graphiteEndpoint = new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
    }
    this.roadName = roadName;
  }

  @Override
  public void configureReporters(MetricRegistry registry) {
    if (graphiteEndpoint != null) {
      log.info("Starting Graphite reporter");
      reporter = registerReporter(GraphiteReporter
          .forRegistry(registry)
          .prefixedWith(MetricRegistry.name("road", "truck-park", "host", getHostname(), "road", roadName))
          .convertRatesTo(SECONDS)
          .convertDurationsTo(MILLISECONDS)
          .build(new Graphite(graphiteEndpoint)));
    }
  }

  private String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroy() throws Exception {
    if (reporter != null) {
      reporter.report();
    }
    super.destroy();
  }

}
