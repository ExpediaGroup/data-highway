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
package com.hotels.road.hive.metastore.s3;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class S3LocationResolverTest {
  @Mock
  private AmazonS3 s3;

  @Test
  public void typical() throws URISyntaxException {
    S3LocationResolver resolver = new S3LocationResolver(s3, "bucket", "prefix");
    URI uri = resolver.resolveLocation("tableName", false);
    assertThat(uri, is(new URI("s3://bucket/prefix/tableName")));
  }

  @Test
  public void noPrefix() throws URISyntaxException {
    S3LocationResolver resolver = new S3LocationResolver(s3, "bucket", null);
    URI uri = resolver.resolveLocation("tableName", false);
    assertThat(uri, is(new URI("s3://bucket/tableName")));
  }

  @Test
  public void create() throws URISyntaxException {
    S3LocationResolver resolver = new S3LocationResolver(s3, "bucket", "prefix");
    resolver.resolveLocation("tableName", true);
    ArgumentCaptor<ByteArrayInputStream> contentCaptor = ArgumentCaptor.forClass(ByteArrayInputStream.class);
    ArgumentCaptor<ObjectMetadata> metadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    verify(s3).putObject(eq("bucket"), eq("prefix/tableName/"), contentCaptor.capture(), metadataCaptor.capture());
    assertThat(contentCaptor.getValue().available(), is(0));
    assertThat(metadataCaptor.getValue().getContentLength(), is(0L));
  }
}
