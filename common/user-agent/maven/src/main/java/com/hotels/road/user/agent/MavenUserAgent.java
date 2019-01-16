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
package com.hotels.road.user.agent;

import static lombok.AccessLevel.PRIVATE;

import java.util.Optional;

import lombok.AllArgsConstructor;

import com.hotels.road.maven.version.MavenVersion;
import com.hotels.road.user.agent.UserAgent.Token;

@AllArgsConstructor(access = PRIVATE)
public final class MavenUserAgent {
  public static Token token(Class<?> aClass, String groupId, String artifactId) {
    return new Token(artifactId, MavenVersion.version(aClass, groupId, artifactId), Optional.empty());
  }
}
