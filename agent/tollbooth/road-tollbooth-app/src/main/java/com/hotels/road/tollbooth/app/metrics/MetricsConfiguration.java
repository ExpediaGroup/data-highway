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
package com.hotels.road.tollbooth.app.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.net.HostAndPort;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import com.ryantenney.metrics.spring.servlets.MetricsServletsContextListener;

@Configuration
@EnableMetrics(proxyTargetClass = true)
@Slf4j
public class MetricsConfiguration extends MetricsConfigurerAdapter {
  private final InetSocketAddress graphiteEndpoint;

  @Autowired
  public MetricsConfiguration(@Value("${graphite.endpoint:disabled}") String graphiteEndpoint) {
    if ("disabled".equalsIgnoreCase(graphiteEndpoint)) {
      log.info("Graphite metrics reporting is disabled");
      this.graphiteEndpoint = null;
    } else {
      log.info("Graphite reporting is configured for {}", graphiteEndpoint);
      HostAndPort hostAndPort = HostAndPort.fromString(graphiteEndpoint);
      this.graphiteEndpoint = new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
    }
  }

  @Override
  public void configureReporters(MetricRegistry registry) {
    try {
      log.info("Adding JVM metric sets to metrics registry");
      registry.register(jvmName("gc"), new GarbageCollectorMetricSet());
      registry.register(jvmName("buffers"), new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
      registry.register(jvmName("memory"), new MemoryUsageGaugeSet());
      registry.register(jvmName("threads"), new ThreadStatesGaugeSet());
    } catch (IllegalArgumentException e) {
      log.warn("Exception adding JVM metric sets", e);
    }

    if (graphiteEndpoint != null) {
      log.info("Starting Graphite reporter");
      registerReporter(GraphiteReporter
          .forRegistry(registry)
          .prefixedWith(MetricRegistry.name("road", "tollbooth", "host", getHostname()))
          .convertRatesTo(SECONDS)
          .convertDurationsTo(MILLISECONDS)
          .build(new Graphite(graphiteEndpoint))).start(10, SECONDS);
    }
  }

  @Bean
  public MetricsServletsContextListener metricsServletsContextListener() {
    return new MetricsServletsContextListener();
  }

  @Bean
  public ServletRegistrationBean<?> adminServletRegistration() {
    return new ServletRegistrationBean<>(new com.codahale.metrics.servlets.AdminServlet(), "/admin/*");
  }

  @Bean
  public ServletRegistrationBean<?> prometheusServletRegistration(MetricRegistry registry) {
    CollectorRegistry.defaultRegistry.register(new DropwizardExports(registry));
    return new ServletRegistrationBean<>(new io.prometheus.client.exporter.MetricsServlet(), "/metrics");
  }

  private String jvmName(String name) {
    return MetricRegistry.name("jvm", name);
  }

  private String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

}
