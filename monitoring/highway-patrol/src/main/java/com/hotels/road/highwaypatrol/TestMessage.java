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
package com.hotels.road.highwaypatrol;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

@Getter
@ToString(exclude = { "payload", "payloadHash" })
public class TestMessage {
  private final String origin;
  private final int group;
  private final long seqNumber;
  private final long timestamp;
  private String payload;

  @JsonIgnore
  private final int payloadHash;

  public TestMessage(
      @JsonProperty("origin") String origin,
      @JsonProperty("group") int group,
      @JsonProperty("seqNumber") long seqNumber,
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("payload") String payload) {
    this.origin = origin;
    this.group = group;
    this.seqNumber = seqNumber;
    this.timestamp = timestamp;
    this.payload = payload;
    payloadHash = payload.hashCode();
  }

  public void clearPayload() {
    payload = null;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(origin)
        .append(group)
        .append(seqNumber)
        .append(timestamp)
        .append(payloadHash)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TestMessage other = (TestMessage) obj;

    return new EqualsBuilder()
        .append(origin, other.origin)
        .append(group, other.group)
        .append(seqNumber, other.seqNumber)
        .append(timestamp, other.timestamp)
        .append(payloadHash, other.payloadHash)
        .build();
  }
}
