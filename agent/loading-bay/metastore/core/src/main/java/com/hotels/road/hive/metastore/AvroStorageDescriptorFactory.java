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
package com.hotels.road.hive.metastore;

import static java.util.Collections.emptyList;

import static lombok.AccessLevel.PRIVATE;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import lombok.AllArgsConstructor;

@AllArgsConstructor(access = PRIVATE)
public final class AvroStorageDescriptorFactory {
  static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  static final String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
  static final String AVRO_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";

  public static StorageDescriptor create(String location) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setInputFormat(AVRO_INPUT_FORMAT);
    storageDescriptor.setOutputFormat(AVRO_OUTPUT_FORMAT);
    storageDescriptor.setLocation(location);
    storageDescriptor.setCols(emptyList());

    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib(AVRO_SERDE);
    storageDescriptor.setSerdeInfo(serdeInfo);

    return storageDescriptor;
  }
}
