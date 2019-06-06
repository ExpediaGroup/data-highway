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
package com.hotels.road.truck.park;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.LongConsumer;

@lombok.RequiredArgsConstructor
class ConsumerCountOutputStream extends OutputStream {
  private final OutputStream delegate;
  private final LongConsumer consumer;

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    delegate.write(b, off, len);
    consumer.accept(len);
  }

  @Override
  public void write(int b) throws IOException {
    delegate.write(b);
    consumer.accept(1);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

}
