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

import static org.apache.commons.lang3.StringUtils.trimToNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import static com.hotels.road.s3.io.S3MultipartOutputStream.MINIMUM_PART_SIZE;

import java.util.concurrent.ExecutorService;

import fm.last.commons.lang.templates.RetryTemplate;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;

import com.hotels.road.io.AbortableOutputStream;

public class S3MultipartOutputStreamBuilder {

  private AmazonS3 s3;
  private String bucket;
  private String key;
  private boolean enableServerSideEncryption;
  private int partSize = MINIMUM_PART_SIZE;
  private int maxAttempts;
  private int sleepSeconds;
  private int poolSize;
  private int queueSize;

  S3MultipartOutputStreamBuilder() {}

  public S3MultipartOutputStreamBuilder s3(AmazonS3 s3, String bucket, String key) {
    this.s3 = s3;
    this.bucket = bucket;
    this.key = key;
    return this;
  }

  public S3MultipartOutputStreamBuilder enableServerSideEncryption(boolean enableServerSideEncryption) {
    this.enableServerSideEncryption = enableServerSideEncryption;
    return this;
  }

  public S3MultipartOutputStreamBuilder partSize(int partSize) {
    this.partSize = partSize;
    return this;
  }

  public S3MultipartOutputStreamBuilder retries(int maxAttempts, int sleepSeconds) {
    this.maxAttempts = maxAttempts;
    this.sleepSeconds = sleepSeconds;
    return this;
  }

  public S3MultipartOutputStreamBuilder async(int poolSize, int queueSize) {
    this.poolSize = poolSize;
    this.queueSize = queueSize;
    return this;
  }

  public AbortableOutputStream build() {
    checkNotNull(s3);
    checkArgument(trimToNull(bucket) != null);
    checkArgument(trimToNull(key) != null);
    checkArgument(partSize >= MINIMUM_PART_SIZE);
    checkArgument(maxAttempts > 0);
    checkArgument(sleepSeconds >= 0);
    checkArgument(poolSize > 0);
    checkArgument(queueSize > 0);

    S3MultipartUpload upload = new S3MultipartUpload(s3, bucket, key, enableServerSideEncryption);
    RetryTemplate retry = new RetryTemplate(maxAttempts, sleepSeconds);
    ExecutorService executor = new BlockingExecutor(poolSize, queueSize);
    AsyncHandler<PartETag> asyncHandler = new AsyncHandler<>(executor);

    return new S3MultipartOutputStream(upload, partSize, retry, asyncHandler);
  }
}
