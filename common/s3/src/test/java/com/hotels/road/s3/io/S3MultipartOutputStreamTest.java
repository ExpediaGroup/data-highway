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
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import fm.last.commons.lang.templates.RetryTemplate;

import com.amazonaws.services.s3.model.PartETag;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

@RunWith(MockitoJUnitRunner.class)
public class S3MultipartOutputStreamTest {

  private static final String UPLOAD_ID = "uploadId";
  private static final String MD5_FOR_BYTE_ZERO = "k7iFrf4NoInN9jSQT9WfcQ==";
  private static final String MD5_FOR_BYTE_ONE = "VaVACK0bpYmqIQ0mKcHfQQ==";
  private static final String MD5_FOR_BYTES_ZERO_ONE = "RBB3zJ5XVU3Udr37i4uBAg==";

  private final RetryTemplate retry = new RetryTemplate(1, 0);
  private final ExecutorService executor = Executors.newFixedThreadPool(2);
  private final AsyncHandler<PartETag> asyncHandler = new AsyncHandler<>(executor);

  @Mock
  private S3MultipartUpload upload;
  @Mock
  private PartETag partETag1;
  @Mock
  private PartETag partETag2;

  @Test
  public void singlePart() throws IOException {
    when(upload.start()).thenReturn(UPLOAD_ID);
    ArgumentCaptor<S3Part> partCaptor = ArgumentCaptor.forClass(S3Part.class);
    when(upload.upload(eq(UPLOAD_ID), partCaptor.capture())).thenReturn(partETag1);

    try (OutputStream output = new S3MultipartOutputStream(upload, 2, retry, asyncHandler)) {
      output.write(new byte[] { 0, 1 });
    }

    List<S3Part> parts = partCaptor.getAllValues();
    assertThat(parts.size(), is(1));

    S3Part part1 = parts.get(0);
    assertThat(part1.getNumber(), is(1));
    assertThat(part1.getSize(), is(2));
    assertThat(part1.getMd5(), is(MD5_FOR_BYTES_ZERO_ONE));
    assertThat(ByteStreams.toByteArray(part1.getInputStream()), is(new byte[] { 0, 1 }));

    verify(upload).complete(UPLOAD_ID, ImmutableList.of(partETag1));
  }

  @Test
  public void twoParts() throws IOException {
    when(upload.start()).thenReturn(UPLOAD_ID);
    ArgumentCaptor<S3Part> partCaptor = ArgumentCaptor.forClass(S3Part.class);
    when(upload.upload(eq(UPLOAD_ID), partCaptor.capture())).thenReturn(partETag1, partETag2);

    try (OutputStream output = new S3MultipartOutputStream(upload, 1, retry, asyncHandler)) {
      output.write(new byte[] { 0, 1 });
    }

    List<S3Part> parts = partCaptor.getAllValues();
    assertThat(parts.size(), is(2));
    Map<Integer, S3Part> map = Maps.uniqueIndex(parts, S3Part::getNumber);

    S3Part part1 = map.get(1);
    assertThat(part1.getNumber(), is(1));
    assertThat(part1.getSize(), is(1));
    assertThat(part1.getMd5(), is(MD5_FOR_BYTE_ZERO));
    assertThat(ByteStreams.toByteArray(part1.getInputStream()), is(new byte[] { 0 }));

    S3Part part2 = map.get(2);
    assertThat(part2.getNumber(), is(2));
    assertThat(part2.getSize(), is(1));
    assertThat(part2.getMd5(), is(MD5_FOR_BYTE_ONE));
    assertThat(ByteStreams.toByteArray(part2.getInputStream()), is(new byte[] { 1 }));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PartETag>> tagCaptor = ArgumentCaptor.forClass(List.class);
    verify(upload).complete(eq(UPLOAD_ID), tagCaptor.capture());
    assertThat(tagCaptor.getValue(), hasSize(2));
    assertThat(tagCaptor.getValue(), containsInAnyOrder(partETag1, partETag2));
  }

  @Test
  public void noDataWritten() throws IOException {
    OutputStream output = new S3MultipartOutputStream(upload, 1, retry, asyncHandler);
    output.close();
    verify(upload, never()).start();
    verify(upload, never()).abort(UPLOAD_ID);
  }

  @Test
  public void errorOnComplete() throws IOException {
    when(upload.start()).thenReturn(UPLOAD_ID);
    ArgumentCaptor<S3Part> partCaptor = ArgumentCaptor.forClass(S3Part.class);
    when(upload.upload(eq(UPLOAD_ID), partCaptor.capture())).thenReturn(partETag1);
    doThrow(Exception.class).when(upload).complete(UPLOAD_ID, ImmutableList.of(partETag1));

    try {
      OutputStream output = new S3MultipartOutputStream(upload, 1, retry, asyncHandler);
      output.write(new byte[] { 0 });
      output.close();
      fail();
    } catch (Exception e) {
      verify(upload).abort(UPLOAD_ID);
    }
  }

  @Test
  public void errorUploadingOnCheckForFailures() throws IOException {
    when(upload.start()).thenReturn(UPLOAD_ID);
    doThrow(Exception.class).when(upload).upload(anyString(), any(S3Part.class));

    try {
      @SuppressWarnings("resource")
      OutputStream output = new S3MultipartOutputStream(upload, 1, retry, asyncHandler);
      output.write(new byte[] { 0 });
      Thread.sleep(100L);
      output.write(new byte[] { 1 });
      fail();
    } catch (Exception e) {
      verify(upload).abort(UPLOAD_ID);
    }
  }

  @Test
  public void errorUploadingOnCompleteUploads() throws IOException {
    when(upload.start()).thenReturn(UPLOAD_ID);
    doThrow(Exception.class).when(upload).upload(anyString(), any(S3Part.class));

    try {
      OutputStream output = new S3MultipartOutputStream(upload, 1, retry, asyncHandler);
      output.write(new byte[] { 0 });
      output.close();
      fail();
    } catch (Exception e) {
      verify(upload).abort(UPLOAD_ID);
    }
  }

  @Test(timeout = 1000L)
  public void failFastUploadingOnCompleteUploads() throws IOException {
    when(upload.start()).thenReturn(UPLOAD_ID);
    when(upload.upload(anyString(), any(S3Part.class))).then(new Answer<PartETag>() {
      @Override
      public PartETag answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(5000L);
        return partETag2;
      }
    }).thenThrow(IOException.class);

    try {
      OutputStream output = new S3MultipartOutputStream(upload, 1, retry, asyncHandler);
      output.write(new byte[] { 0 });
      output.write(new byte[] { 1 });
      output.close();
      fail();
    } catch (Exception e) {
      verify(upload).abort(UPLOAD_ID);
    }
  }

}
