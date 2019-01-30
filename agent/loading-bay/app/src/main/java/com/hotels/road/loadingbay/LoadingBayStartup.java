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
package com.hotels.road.loadingbay;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;

import com.hotels.road.s3.io.S3ConnectivityCheck;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LoadingBayStartup {

  @Autowired
  private AmazonS3 s3;
  @Value("${hive.table.schema.bucket}")
  private String bucket;
  @Value("${hive.table.schema.prefix}")
  private String s3KeyPrefix;

  /**
   * Check connections before to start the application.
   * In case of a problem, it stops the application.
   */
  @PostConstruct
  public void postConstruct() {
    try {
      new S3ConnectivityCheck().checkS3Put(s3, bucket, s3KeyPrefix + "/.test");
    } catch (Exception e) {
      log.error("Application is going to be stopped.");
      throw e;
    }
  }
}
