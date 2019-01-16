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
package com.hotels.road.loadingbay.lander;

import java.util.Map;

import lombok.Data;
import lombok.experimental.Wither;

@Data
public class LanderConfiguration {
  private final String roadName;
  private final String topicName;
  private final @Wither Map<Integer, OffsetRange> offsets;
  private final String s3KeyPrefix;
  private final boolean enableServerSideEncryption;
  private final String acquisitionInstant;
  private final boolean runAgain;
}
