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

import static fm.last.commons.lang.units.IecByteUnit.MEBIBYTES;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import fm.last.commons.lang.templates.RetryTemplate;
import fm.last.commons.lang.templates.RetryTemplate.RetryCallback;
import lombok.RequiredArgsConstructor;

import com.amazonaws.services.s3.model.PartETag;

import com.hotels.road.io.AbortableOutputStream;

/**
 * An {@link OutputStream} wrapper for performing a S3 multipart uploads with support for retries and asynchronous
 * uploads of individual parts.
 */
@RequiredArgsConstructor
public class S3MultipartOutputStream extends AbortableOutputStream {
  public static final int MINIMUM_PART_SIZE = (int) MEBIBYTES.toBytes(5L);

  private final S3MultipartUpload upload;
  private final int partSize;
  private final RetryTemplate retry;
  private final AsyncHandler<PartETag> asyncHandler;

  private String uploadId;
  private int partNumber = 1;
  private S3PartOutputStream part;
  private boolean closedOrAborted = false;

  @Override
  public void write(int b) throws IOException {
    if (part == null) {
      if (partNumber == 1) {
        uploadId = upload.start();
      }
      part = new S3PartOutputStream(partSize, partNumber++);
    }
    part.write(b);
    if (part.size() >= partSize) {
      performUpload();
    }
  }

  private void performUpload() {
    if (part != null) {
      S3Part s3Part = part.s3Part();
      asyncHandler.supply(() -> performUpload(s3Part));
      part = null;
      try {
        asyncHandler.checkForFailures();
      } catch (Exception e) {
        upload.abort(uploadId);
        throw e;
      }
    }
  }

  private PartETag performUpload(S3Part s3Part) {
    try {
      return retry.execute(new RetryCallback<PartETag>() {
        @Override
        public PartETag doWithRetry() throws Exception {
          return upload.upload(uploadId, s3Part);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!closedOrAborted) {
      performUpload();
      try {
        List<PartETag> partETags = asyncHandler.waitForCompletion();
        if (partETags.size() > 0) {
          upload.complete(uploadId, partETags);
        }
      } catch (Exception e) {
        upload.abort(uploadId);
        throw e;
      } finally {
        asyncHandler.close();
      }
      closedOrAborted = true;
    }
  }

  @Override
  public void abort() throws IOException {
    if (!closedOrAborted) {
      asyncHandler.cancel();
      upload.abort(uploadId);
      closedOrAborted = true;
    }
  }

  public static S3MultipartOutputStreamBuilder builder() {
    return new S3MultipartOutputStreamBuilder();
  }
}
