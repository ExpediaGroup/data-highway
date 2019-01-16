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
package com.hotels.road.hive.metastore.s3;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Charsets;

import com.hotels.road.hive.metastore.MetaStoreException;

@RunWith(MockitoJUnitRunner.class)
public class S3SchemaUriResolverTest {

  private static final Schema SCHEMA = SchemaBuilder.record("record").fields().requiredLong("id").endRecord();
  private static final int VERSION = 1;
  private static final String ROAD = "road";
  private static final String PREFIX = "prefix";
  private static final String BUCKET = "bucket";

  @Mock
  private TransferManager transferManager;
  @Mock
  private Upload upload;
  @Captor
  private ArgumentCaptor<InputStream> uploadCaptor;
  @Captor
  private ArgumentCaptor<ObjectMetadata> metadataCaptor;

  private S3SchemaUriResolver uriResolver;

  @Before
  public void injectMocks() {
    uriResolver = new S3SchemaUriResolver(transferManager, S3SchemaUriResolver.S3_URI_FORMAT, BUCKET, PREFIX, false);
  }

  @Test
  public void resolve() throws Exception {
    when(transferManager.upload(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class)))
        .thenReturn(upload);

    URI schemaUri = uriResolver.resolve(SCHEMA, ROAD, VERSION);

    verify(transferManager).upload(eq(BUCKET), eq("prefix/roads/road/schemas/1/road_v1.avsc"), uploadCaptor.capture(),
        metadataCaptor.capture());
    verify(upload).waitForCompletion();

    InputStream uploadStream = uploadCaptor.getValue();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copy(uploadStream, out);
    String schemaString = new String(out.toByteArray(), Charsets.UTF_8);
    assertThat(schemaString, is(SCHEMA.toString()));

    ObjectMetadata objectMetadata = metadataCaptor.getValue();
    assertThat((int) objectMetadata.getContentLength(), is(out.toByteArray().length));

    assertThat(schemaUri, is(new URI("s3://bucket/prefix/roads/road/schemas/1/road_v1.avsc")));
  }

  @Test(expected = MetaStoreException.class)
  public void resolveAmazonServiceExceptionOnUpload() throws Exception {
    when(transferManager.upload(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class)))
        .thenThrow(AmazonServiceException.class);

    uriResolver.resolve(SCHEMA, ROAD, VERSION);
  }

  @Test(expected = MetaStoreException.class)
  public void resolveAmazonClientExceptionOnUpload() throws Exception {
    when(transferManager.upload(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class)))
        .thenThrow(AmazonClientException.class);

    uriResolver.resolve(SCHEMA, ROAD, VERSION);
  }

  @Test(expected = MetaStoreException.class)
  public void resolveIOExceptionOnUpload() throws Exception {
    when(transferManager.upload(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class)))
        .thenThrow(IOException.class);

    uriResolver.resolve(SCHEMA, ROAD, VERSION);
  }

  @Test(expected = MetaStoreException.class)
  public void resolveInterruptedExceptionOnUpload() throws Exception {
    when(transferManager.upload(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class)))
        .thenThrow(InterruptedException.class);

    uriResolver.resolve(SCHEMA, ROAD, VERSION);
  }

  @Test
  public void newKey() {
    String key = uriResolver.newKey(ROAD, VERSION);
    assertThat(key, is("prefix/roads/road/schemas/1/road_v1.avsc"));
  }

  @Test
  public void newKeyNoPrefix() {
    uriResolver = new S3SchemaUriResolver(transferManager, S3SchemaUriResolver.S3_URI_FORMAT, BUCKET, null, false);

    String key = uriResolver.newKey(ROAD, VERSION);
    assertThat(key, is("roads/road/schemas/1/road_v1.avsc"));
  }

  @Test
  public void newS3Uri() throws URISyntaxException {
    URI uri = uriResolver.newS3Uri("x");
    assertThat(uri, is(new URI("s3://bucket/x")));
  }

  @Test(expected = MetaStoreException.class)
  public void newS3UriException() throws URISyntaxException {
    uriResolver = new S3SchemaUriResolver(transferManager, "NOT_VALID_URI:{{}", BUCKET, null, false);
    uriResolver.newS3Uri("x");
  }

}
