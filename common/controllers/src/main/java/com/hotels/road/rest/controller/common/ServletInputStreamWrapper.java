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
package com.hotels.road.rest.controller.common;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

class ServletInputStreamWrapper extends ServletInputStream {
  private final InputStream content;

  public ServletInputStreamWrapper(InputStream content) {
    this.content = content;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return content.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return content.read(b, off, len);
  }

  @Override
  public int read() throws IOException {
    return content.read();
  }

  @Override
  public boolean isFinished() {
    try {
      return content.available() == 0;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public void setReadListener(ReadListener listener) {
    throw new UnsupportedOperationException();
  }
}
