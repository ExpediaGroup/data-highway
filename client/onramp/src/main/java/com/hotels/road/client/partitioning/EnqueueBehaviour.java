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
package com.hotels.road.client.partitioning;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

enum EnqueueBehaviour {
  COMPLETE_EXCEPTIONALLY() {
    @Override
    public <MESSAGE, RESPONSE> CompletableFuture<RESPONSE> enqueueMessage(
        BlockingQueue<MessageWithCallback<MESSAGE, RESPONSE>> queue,
        MESSAGE message) {
      MessageWithCallback<MESSAGE, RESPONSE> messageWithCallback = new MessageWithCallback<>(message);
      CompletableFuture<RESPONSE> callback = messageWithCallback.getCallback();
      if (!queue.offer(messageWithCallback)) {
        callback.completeExceptionally(new IllegalStateException("Send buffer is full"));
      }
      return callback;
    }
  },
  BLOCK_AND_WAIT() {
    @Override
    public <MESSAGE, RESPONSE> CompletableFuture<RESPONSE> enqueueMessage(
        BlockingQueue<MessageWithCallback<MESSAGE, RESPONSE>> queue,
        MESSAGE message)
      throws InterruptedException {
      MessageWithCallback<MESSAGE, RESPONSE> messageWithCallback = new MessageWithCallback<>(message);
      queue.put(messageWithCallback);
      return messageWithCallback.getCallback();
    }
  };

  public abstract <MESSAGE, RESPONSE> CompletableFuture<RESPONSE> enqueueMessage(
      BlockingQueue<MessageWithCallback<MESSAGE, RESPONSE>> queue,
      MESSAGE message)
    throws InterruptedException;
}
