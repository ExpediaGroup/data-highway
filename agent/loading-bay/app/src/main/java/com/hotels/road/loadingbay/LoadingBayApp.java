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
package com.hotels.road.loadingbay;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import io.micrometer.core.instrument.MeterRegistry;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.agents.trafficcop.TrafficCopConfiguration;
import com.hotels.road.boot.DataHighwayApplication;
import com.hotels.road.hive.metastore.AvroHiveTableStrategy;
import com.hotels.road.hive.metastore.HiveConfFactory;
import com.hotels.road.hive.metastore.HivePartitionManager;
import com.hotels.road.hive.metastore.HiveTableManager;
import com.hotels.road.hive.metastore.HiveTableStrategy;
import com.hotels.road.hive.metastore.LocationResolver;
import com.hotels.road.hive.metastore.MetaStoreClientFactory;
import com.hotels.road.hive.metastore.SchemaUriResolver;
import com.hotels.road.hive.metastore.s3.S3LocationResolver;
import com.hotels.road.hive.metastore.s3.S3SchemaUriResolver;
import com.hotels.road.loadingbay.event.HiveNotificationHandler;
import com.hotels.road.loadingbay.lander.Lander;
import com.hotels.road.loadingbay.lander.kubernetes.KubernetesConfiguration;
import com.hotels.road.loadingbay.model.Destinations;
import com.hotels.road.loadingbay.model.Hive;
import com.hotels.road.loadingbay.model.HiveRoad;
import com.hotels.road.loadingbay.model.HiveStatus;
import com.hotels.road.notification.NotificationConfiguration;
import com.hotels.road.notification.sns.config.SnsConfiguration;
import com.hotels.road.schema.serde.SchemaSerializationModule;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@SpringBootApplication
@EnableScheduling
@Import({
    KubernetesConfiguration.class,
    TrafficCopConfiguration.class,
    NotificationConfiguration.class,
    SnsConfiguration.class })
public class LoadingBayApp {
  @Bean
  public ObjectMapper mapper() {
    return new ObjectMapper().registerModule(new SchemaSerializationModule());
  }

  @Bean
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
  public IMetaStoreClient metaStoreClient(
      @Value("${hive.metastore.uris}") String hiveMetaStoreUris,
      @Value("${hive.thrift.failureRetries:3}") int failureRetries,
      @Value("${hive.thrift.connectRetryDelay:10}") long connectRetryDelay) {
    HiveConf hiveConf = new HiveConfFactory().newInstance(hiveMetaStoreUris);
    hiveConf.setIntVar(METASTORETHRIFTFAILURERETRIES, failureRetries);
    hiveConf.setTimeVar(METASTORE_CLIENT_CONNECT_RETRY_DELAY, connectRetryDelay, TimeUnit.SECONDS);
    return new MetaStoreClientFactory().newInstance(hiveConf);
  }

  @Bean
  public LocationResolver locationResolver(@Value("${hive.table.location.bucket}") String locationBucket) {
    return new S3LocationResolver(locationBucket, "");
  }

  @Bean
  public SchemaUriResolver schemaUriResolver(
      AmazonS3 s3Client,
      @Value("${hive.table.schema.bucket}") String schemaBucket,
      @Value("${hive.table.schema.prefix}") String schemaPrefix,
      @Value("${s3.enableServerSideEncryption:false}") boolean enableServerSideEncryption) {
    return new S3SchemaUriResolver(s3Client, schemaBucket, schemaPrefix, enableServerSideEncryption);
  }

  @Bean
  public HiveTableManager hiveTableManager(
      IMetaStoreClient metaStoreClient,
      HiveTableStrategy hiveTableStrategy,
      LocationResolver locationResolver,
      @Value("${hive.database}") String databaseName) {
    return new HiveTableManager(metaStoreClient, hiveTableStrategy, locationResolver, databaseName);
  }

  @Bean
  public HiveTableStrategy hiveTableStrategy(SchemaUriResolver schemaUriResolver, Clock clock) {
    return new AvroHiveTableStrategy(schemaUriResolver, clock);
  }

  @Bean
  public HivePartitionManager hivePartitionManager(
      IMetaStoreClient metaStoreClient,
      LocationResolver locationResolver,
      @Value("${hive.database}") String database,
      Clock clock) {
    return new HivePartitionManager(metaStoreClient, locationResolver, database, clock);
  }

  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  public Function<HiveRoad, LanderMonitor> monitorFactory(
      Clock clock,
      MeterRegistry meterRegistry,
      PatchSetEmitter emitter,
      @Value("#{landerFactory}") Lander.Factory landerFactory,
      @Value("${hive.table.schema.bucket}") String bucketName,
      @Value("${hive.database}") String database,
      OffsetManager offsetManager,
      HivePartitionManager hivePartitionManager,
      HiveNotificationHandler landingHandler,
      @Value("${maxRecordsPerPartition:100000}") long maxRecordsPerPartition,
      @Value("${s3.enableServerSideEncryption:false}") boolean enableServerSideEncryption,
      @Value("${jitter:true}") boolean jitter) {
    return road -> {
      LanderTaskRunner runnable = new LanderTaskRunner(meterRegistry, offsetManager, road.getName(),
          road.getTopicName(), database, hivePartitionManager, landerFactory, landingHandler, emitter, clock,
          maxRecordsPerPartition, enableServerSideEncryption);
      OffsetDateTime landerLastRun = Optional
          .ofNullable(road.getDestinations())
          .map(Destinations::getHive)
          .map(Hive::getStatus)
          .map(HiveStatus::getLastRun)
          .orElse(LoadingBay.EPOCH);
      return new LanderMonitor(clock, runnable, landerLastRun, jitter);
    };
  }

  public static void main(String[] args) {
    DataHighwayApplication.run(LoadingBayApp.class, args);
  }
}
