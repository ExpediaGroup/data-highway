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

import java.io.InputStream;

/**
 * Simple container class for holding items required by {@link S3MultipartUpload#upload(String, S3Part)}.
 */
@lombok.RequiredArgsConstructor
@lombok.Getter
class S3Part {
  private final int number;
  private final int size;
  private final String md5;
  private final InputStream inputStream;
}
