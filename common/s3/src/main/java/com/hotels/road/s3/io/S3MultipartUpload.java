/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Stopwatch;

/**
 * This class encapsulates the AWS SDK operations required to complete a multipart upload.
 */
@Slf4j
@lombok.RequiredArgsConstructor
class S3MultipartUpload {
  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private final AmazonS3 s3;
  private final String bucket;
  private final String key;
  private final boolean enableServerSideEncryption;

  private long bytes = 0;

  String start() {
    bytes = 0;
    InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, key);

    if(enableServerSideEncryption) {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
      request.setObjectMetadata(objectMetadata);
    }

    String uploadId = s3.initiateMultipartUpload(request).getUploadId();
    stopwatch.start();
    log.info("Starting upload to s3://{}/{}.", bucket, key);
    return uploadId;
  }

  PartETag upload(String uploadId, S3Part part) {
    Object[] logParams = new Object[] { part.getSize(), part.getNumber(), bucket, key };
    log.debug("Uploading {} bytes for part {} to s3://{}/{}.", logParams);
    UploadPartRequest request = new UploadPartRequest()
        .withUploadId(uploadId)
        .withBucketName(bucket)
        .withKey(key)
        .withPartNumber(part.getNumber())
        .withPartSize(part.getSize())
        .withMD5Digest(part.getMd5())
        .withInputStream(part.getInputStream());
    UploadPartResult result = s3.uploadPart(request);
    log.debug("Uploaded {} bytes for part {} to s3://{}/{}.", logParams);
    bytes += part.getSize();
    return result.getPartETag();
  }

  void complete(String uploadId, List<PartETag> partETags) {
    CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags);
    s3.completeMultipartUpload(request);
    if (stopwatch.isRunning()) {
      stopwatch.stop();
    }
    long seconds = stopwatch.elapsed(SECONDS);
    log.info("Successfully uploaded {} bytes in {} seconds ({} bps) to s3://{}/{}", bytes, seconds,
        (float) bytes / seconds, bucket, key);
  }

  void abort(String uploadId) {
    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(bucket, key, uploadId);
    s3.abortMultipartUpload(request);
    log.warn("Aborted upload to s3://{}/{}.", bucket, key);
  }

}
