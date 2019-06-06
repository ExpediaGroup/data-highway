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
package com.hotels.road.maven.version;

import static lombok.AccessLevel.PRIVATE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;
import java.util.Properties;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(access = PRIVATE)
public final class MavenVersion {
  public static String version(@NonNull Class<?> aClass, @NonNull String groupId, @NonNull String artifactId) {
    String pomProperties = String.format("META-INF/maven/%s/%s/pom.properties", groupId, artifactId);
    return Optional
        .of(pomProperties)
        .map(aClass.getClassLoader()::getResource)
        .map(MavenVersion::loadProperties)
        .map(p -> p.getProperty("version"))
        .orElseGet(() -> {
          log.warn("Unable to read {}", pomProperties);
          return "unknown";
        });
  }

  private static Properties loadProperties(URL url) {
    Properties properties = new Properties();
    try (InputStream input = url.openStream()) {
      properties.load(input);
    } catch (IOException ignore) {}
    return properties;
  }
}
