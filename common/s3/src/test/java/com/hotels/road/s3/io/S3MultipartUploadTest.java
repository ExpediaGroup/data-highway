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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

@RunWith(MockitoJUnitRunner.class)
public class S3MultipartUploadTest {

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";
  private static final String UPLOAD_ID = "uploadId";

  @Mock
  private AmazonS3 s3;

  private S3MultipartUpload underTest;

  @Before
  public void before() {
    underTest = new S3MultipartUpload(s3, BUCKET, KEY, false);
  }

  @Test
  public void start() {
    ArgumentCaptor<InitiateMultipartUploadRequest> request = ArgumentCaptor
        .forClass(InitiateMultipartUploadRequest.class);
    InitiateMultipartUploadResult response = mock(InitiateMultipartUploadResult.class);
    when(response.getUploadId()).thenReturn(UPLOAD_ID);
    when(s3.initiateMultipartUpload(request.capture())).thenReturn(response);

    String result = underTest.start();

    assertThat(result, is(UPLOAD_ID));
    assertThat(request.getValue().getBucketName(), is(BUCKET));
    assertThat(request.getValue().getKey(), is(KEY));
  }

  @Test
  public void upload() {
    ArgumentCaptor<UploadPartRequest> request = ArgumentCaptor.forClass(UploadPartRequest.class);
    UploadPartResult response = mock(UploadPartResult.class);
    PartETag partETag = mock(PartETag.class);
    when(response.getPartETag()).thenReturn(partETag);
    when(s3.uploadPart(request.capture())).thenReturn(response);
    InputStream inputStream = mock(InputStream.class);
    S3Part part = new S3Part(1, 2, "md5", inputStream);

    PartETag result = underTest.upload(UPLOAD_ID, part);

    assertThat(result, is(partETag));
    assertThat(request.getValue().getBucketName(), is(BUCKET));
    assertThat(request.getValue().getKey(), is(KEY));
    assertThat(request.getValue().getPartNumber(), is(1));
    assertThat(request.getValue().getPartSize(), is(2L));
    assertThat(request.getValue().getMd5Digest(), is("md5"));
    assertThat(request.getValue().getInputStream(), is(inputStream));
  }

  @Test
  public void complete() {
    InitiateMultipartUploadResult response = mock(InitiateMultipartUploadResult.class);
    when(response.getUploadId()).thenReturn(UPLOAD_ID);
    when(s3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(response);

    ArgumentCaptor<CompleteMultipartUploadRequest> request = ArgumentCaptor
        .forClass(CompleteMultipartUploadRequest.class);
    @SuppressWarnings("unchecked")
    List<PartETag> partETags = mock(List.class);

    when(s3.completeMultipartUpload(request.capture())).thenReturn(null);

    underTest.start();
    underTest.complete(UPLOAD_ID, partETags);

    assertThat(request.getValue().getBucketName(), is(BUCKET));
    assertThat(request.getValue().getKey(), is(KEY));
    assertThat(request.getValue().getUploadId(), is(UPLOAD_ID));
    assertThat(request.getValue().getPartETags(), is(partETags));
  }

  @Test
  public void abort() {
    ArgumentCaptor<AbortMultipartUploadRequest> request = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);

    doNothing().when(s3).abortMultipartUpload(request.capture());

    underTest.abort(UPLOAD_ID);

    assertThat(request.getValue().getBucketName(), is(BUCKET));
    assertThat(request.getValue().getKey(), is(KEY));
    assertThat(request.getValue().getUploadId(), is(UPLOAD_ID));
  }

}
