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
package com.hotels.road.s3.io;

import java.io.ByteArrayInputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3ConnectivityCheck {

  /**
   * Checking S3 connection with a put request.
   * This function throws a runtime exception if something goes wrong.
   *
   * @param s3
   * @param bucket
   * @param key
   */
  public void checkS3Put(AmazonS3 s3, String bucket, String key) {
    try {
      byte[] source = { 0x0 };
      ByteArrayInputStream is = new ByteArrayInputStream(source);

      final PutObjectRequest object = new PutObjectRequest(bucket, key, is, new ObjectMetadata());
      s3.putObject(object);
    } catch (Exception e) {
      log.error("Unable to write an object to AWS S3: bucket={}, key={}.", bucket, key);
      throw e;
    }
  }
}
