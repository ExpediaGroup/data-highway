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
package com.hotels.road.notification.sns.config;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;

import com.hotels.road.notification.NotificationSender;
import com.hotels.road.notification.sns.SnsNotificationSender;

@Configuration
@Slf4j
public class SnsConfiguration {
  @Bean
  public AmazonSNSAsync snsClient(
      @Value("${notification.sns.region}") String region,
      @Value("${notification.sns.endpointUrl:disabled}") String snsEndpointUrl) {
    if ("disabled".equalsIgnoreCase(snsEndpointUrl)) {
      return null;
    }
    AmazonSNSAsync client = AmazonSNSAsyncClientBuilder
        .standard()
        .withClientConfiguration(
            new ClientConfiguration().withRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicy()))
        .withExecutorFactory(() -> Executors.newSingleThreadScheduledExecutor())
        .withEndpointConfiguration(new EndpointConfiguration(snsEndpointUrl, region))
        .build();
    return client;
  }

  @Bean
  public NotificationSender snsNotificationSender(
      AmazonSNSAsync sns,
      @Value("${notification.sns.topicArnFormat:disabled}") String topicArnFormat) {
    if (sns == null || topicArnFormat == null || "disabled".equalsIgnoreCase(topicArnFormat)) {
      log.info("Data Highway notifications are disabled. (SNS client is: {}, SNS topic is: {})", sns, topicArnFormat);
      return NotificationSender.nullObject;
    }
    return new SnsNotificationSender(sns, n -> String.format(topicArnFormat, n.getRoadName()),
        (t, n) -> String.format("Data Highway Notification for road '%s'.", n.getRoadName()));
  }
}
