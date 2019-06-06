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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Base64;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

class S3PartOutputStream extends ByteArrayOutputStream {

  private final int number;
  private final Hasher hasher;

  @SuppressWarnings("deprecation")
  S3PartOutputStream(int size, int number) {
    super(size);
    this.number = number;
    hasher = Hashing.md5().newHasher(size);
  }

  @Override
  public void write(int b) {
    hasher.putByte((byte) b);
    super.write(b);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) {
    hasher.putBytes(b, off, len);
    super.write(b, off, len);
  }

  S3Part s3Part() {
    return new S3Part(number, size(), md5(), inputStream());
  }

  private String md5() {
    return Base64.getEncoder().encodeToString(hasher.hash().asBytes());
  }

  private InputStream inputStream() {
    return new ByteArrayInputStream(buf, 0, count);
  }

}
