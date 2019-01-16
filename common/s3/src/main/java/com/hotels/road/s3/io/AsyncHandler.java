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
package com.hotels.road.s3.io;

import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import com.amazonaws.services.s3.model.PartETag;

/**
 * This class is designed to handle the asynchronous uploading of individual parts, failing fast when errors occur and
 * gathering the upload response metadata ({@link PartETag PartEtags}) to return back to the caller.
 */
@lombok.RequiredArgsConstructor
class AsyncHandler<T> implements Closeable {
  private final ExecutorService executor;

  private final CompletableFuture<?> anyException = new CompletableFuture<>();
  private final List<CompletableFuture<T>> futures = new ArrayList<>();

  void supply(Supplier<T> supplier) {
    futures.add(supplyAsync(supplier, executor).exceptionally(t -> {
      anyException.completeExceptionally(t);
      return null;
    }));
  }

  void checkForFailures() {
    if (anyException.isCompletedExceptionally()) {
      anyException.join();
    }
  }

  List<T> waitForCompletion() {
    CompletableFuture<List<T>> allSucceed = supplyAsync(
        () -> futures.stream().map(CompletableFuture::join).collect(toList()));
    anyOf(anyException, allSucceed).join();
    return allSucceed.join();
  }

  public void cancel() throws IOException {
    futures.forEach(f -> f.cancel(true));
    close();
  }

  @Override
  public void close() throws IOException {
    executor.shutdown();
  }
}
