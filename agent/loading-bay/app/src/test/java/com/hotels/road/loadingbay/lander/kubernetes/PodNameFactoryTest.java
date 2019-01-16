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
package com.hotels.road.loadingbay.lander.kubernetes;

import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.regex.Pattern;

import org.junit.Test;

import com.hotels.road.loadingbay.lander.LanderConfiguration;

public class PodNameFactoryTest {

  private final Pattern namePattern = Pattern.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$");

  @Test
  public void typical() {
    PodNameFactory factory = new PodNameFactory();
    LanderConfiguration config = new LanderConfiguration("road_name", "road.road_name", singletonMap(0, null),
        "s3KeyPrefix", false, "partitionColumnValue10T28929Z", false);
    String name = factory.newName(config);
    assertThat(name, is("truck-park-road-name-partitioncolumnvalue10t28929z-0"));
    assertThat(namePattern.matcher(name).matches(), is(true));
  }

  @Test
  public void conf() {
    PodNameFactory factory = new PodNameFactory();
    LanderConfiguration configuration = new LanderConfiguration("road_name", "topicName", singletonMap(0, null),
        "s3KeyPrefix", false, "partitionColumnValue102T8929Z", false);
    String name = factory.newName(configuration);
    assertThat(name, is("truck-park-road-name-partitioncolumnvalue102t8929z-0"));
    assertThat(namePattern.matcher(name).matches(), is(true));
  }

  @Test
  public void road_name_too_long_gets_truncated() throws Exception {
    PodNameFactory factory = new PodNameFactory();
    LanderConfiguration configuration = new LanderConfiguration("ten-------twenty----thirty----forty-----fifty-----",
        "topicName", singletonMap(0, null), "s3KeyPrefix", false, "ninechars", false);
    String name = factory.newName(configuration);
    assertThat(name.length(), is(63));
    assertThat(namePattern.matcher(name).matches(), is(true));
  }

  @Test(expected = IllegalStateException.class)
  public void partition_column_value_is_too_long_throws_IllegalStateException() throws Exception {
    PodNameFactory factory = new PodNameFactory();
    LanderConfiguration configuration = new LanderConfiguration("road_name", "topicName", singletonMap(0, null),
        "s3KeyPrefix", false, "ten-------twenty----thirty----forty-----fifty-----sixty-----", false);
    factory.newName(configuration);
  }
}
