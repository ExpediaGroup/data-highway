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
package com.hotels.road.highwaypatrol;

enum MessageState {
  CREATED(false, "created"),
  SENT(false, "sent"),
  SEND_FAILURE(false, "send_failure"),
  SEND_SUCCESS(true, "send_success"),
  SEND_REJECTED(false, "send_rejected"),
  RECEIVED_CORRECT(true, "received_correct"),
  RECEIVED_OUT_OF_ORDER(true, "out_of_order"),
  RECEIVED_CORRUPTED(true, "received_corrupted");

  private final String metricName;
  private final boolean onHighway;

  MessageState(boolean onHighway, String metricName) {
    this.onHighway = onHighway;
    this.metricName = metricName;
  }

  public boolean isOnHighway() {
    return onHighway;
  }

  public String getMetricName() {
    return metricName;
  }
}
