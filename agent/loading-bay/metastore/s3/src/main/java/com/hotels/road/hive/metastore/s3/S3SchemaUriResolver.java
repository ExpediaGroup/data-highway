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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

import com.hotels.road.hive.metastore.MetaStoreException;
import com.hotels.road.hive.metastore.SchemaUriResolver;

/**
 * Stores {@link Schema Schemas} in S3 so that they can be retrieved by Hive's {@code AvroSerDe} using the
 * {@code avro.schema.url} table/partition parameter.
 */
public class S3SchemaUriResolver implements SchemaUriResolver {

  @VisibleForTesting
  static final String S3_URI_FORMAT = "s3://%s/%s";

  private final TransferManager transferManager;
  private final String uriFormat;
  private final String bucket;
  private final String keyPrefix;
  private final boolean enableServerSideEncryption;

  public S3SchemaUriResolver(AmazonS3 s3Client, String bucket, String keyPrefix, boolean enableServerSideEncryption) {
    this(TransferManagerBuilder.standard().withS3Client(s3Client).build(), S3_URI_FORMAT, bucket, keyPrefix,
        enableServerSideEncryption);
  }

  @VisibleForTesting
  S3SchemaUriResolver(
      TransferManager transferManager,
      String uriFormat,
      String bucket,
      String keyPrefix,
      boolean enableServerSideEncryption) {
    this.transferManager = transferManager;
    this.uriFormat = uriFormat;
    this.bucket = bucket;
    this.keyPrefix = keyPrefix;
    this.enableServerSideEncryption = enableServerSideEncryption;
  }

  /**
   * Stores the {@link Schema} in S3, returning a {@link URI} to the created resource. The {@link URI} uses the
   * {@code s3://} scheme and is intended to be navigated by the S3 {@link FileSystem} implementations. Blocks until the
   * schema has been uploaded, providing read-after-write consistency.
   */
  @Override
  public URI resolve(Schema schema, String road, int version) {
    String key = newKey(road, version);
    byte[] bytes = schema.toString().getBytes(Charsets.UTF_8);
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(bytes.length);
    if (enableServerSideEncryption) {
      objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }
    try (InputStream in = new ByteArrayInputStream(bytes)) {
      Upload upload = transferManager.upload(bucket, key, in, objectMetadata);
      upload.waitForCompletion();
    } catch (IOException e) {
      throw new MetaStoreException(String.format("Error closing schema stream: bucket='%s', key='%s'", bucket, key), e);
    } catch (AmazonClientException | InterruptedException e) {
      throw new MetaStoreException(String.format("Error uploading schema: bucket='%s', key='%s'", bucket, key), e);
    }
    return newS3Uri(key);
  }

  @VisibleForTesting
  String newKey(String road, int version) {
    List<String> elements = new ArrayList<>();
    if (StringUtils.isNotBlank(keyPrefix)) {
      elements.add(keyPrefix);
    }
    elements.add("roads");
    elements.add(road);
    elements.add("schemas");
    elements.add(Integer.toString(version));
    elements.add(road + "_v" + version + ".avsc");
    String key = String.join("/", elements);
    return key;
  }

  /*
   * 'Because the pattern is so simple, we've never added a get_bucket_url() method.'
   * https://forums.aws.amazon.com/thread.jspa?threadID=93828
   */
  @VisibleForTesting
  URI newS3Uri(String key) {
    try {
      return new URI(String.format(uriFormat, bucket, key));
    } catch (URISyntaxException e) {
      throw new MetaStoreException(String
          .format("Could not generate URI for schema: format='%s', bucket='%s', key='%s'", uriFormat, bucket, key), e);
    }
  }

}
