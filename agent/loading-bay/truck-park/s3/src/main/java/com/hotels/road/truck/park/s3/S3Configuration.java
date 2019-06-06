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
package com.hotels.road.truck.park.s3;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import com.hotels.road.s3.io.S3MultipartOutputStream;
import com.hotels.road.truck.park.spi.AbortableOutputStreamFactory;

@Configuration
public class S3Configuration {

  @Bean
  @Lazy
  AmazonS3 s3(
      @Value("${s3.endpoint.url}") String s3EndpointUrl,
      @Value("${s3.endpoint.signingRegion}") String signingRegion) {
    return AmazonS3Client
        .builder()
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .withEndpointConfiguration(new EndpointConfiguration(s3EndpointUrl, signingRegion))
        .build();
  }

  @Bean
  AbortableOutputStreamFactory s3OutputStreamFactory(
      AmazonS3 s3,
      @Value("${s3.bucket}") String bucket,
      @Value("${s3.partSize:5242880}") int partSize, // default 5 mebibytes
      @Value("${s3.enableServerSideEncryption:false}") boolean enableServerSideEncryption,
      @Value("${s3.retry.maxAttempts:3}") int maxAttempts,
      @Value("${s3.retry.sleepSeconds:1}") int sleepSeconds,
      @Value("${s3.async.poolSize:3}") int poolSize,
      @Value("${s3.async.queueSize:3}") int queueSize) {
    return key -> S3MultipartOutputStream
        .builder()
        .s3(s3, bucket, key)
        .partSize(partSize)
        .enableServerSideEncryption(enableServerSideEncryption)
        .retries(maxAttempts, sleepSeconds)
        .async(poolSize, queueSize)
        .build();
  }

}
