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
package com.hotels.road.hive.metastore.s3;

import static java.util.stream.Collectors.joining;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.hotels.road.hive.metastore.LocationResolver;
import com.hotels.road.hive.metastore.MetaStoreException;

import org.apache.commons.lang.StringUtils;

@RequiredArgsConstructor
public class S3LocationResolver implements LocationResolver {
  private final AmazonS3 s3;
  private final String bucket;
  private final String prefix;

  @Override
  public URI resolveLocation(String location, boolean create) {
    List<String> elements = new ArrayList<>();
    if (StringUtils.isNotBlank(prefix)) {
      elements.add(prefix);
    }
    elements.add(location);
    String path = elements.stream().collect(joining("/"));

    if (create) {
      ByteArrayInputStream content = new ByteArrayInputStream(new byte[0]);
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(0);
      s3.putObject(bucket, path + "/", content, metadata);
    }

    try {
      return new URI("s3", bucket, "/" + path, null);
    } catch (URISyntaxException e) {
      throw new MetaStoreException(
          String.format("Could not construct location URI for: bucket='%s', prefix='%s', location='%s'", bucket, prefix,
              location),
          e);
    }
  }
}
