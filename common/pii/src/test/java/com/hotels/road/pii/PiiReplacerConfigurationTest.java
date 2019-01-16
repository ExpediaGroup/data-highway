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
package com.hotels.road.pii;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.mock.env.MockEnvironment;

public class PiiReplacerConfigurationTest {
  @Test
  public void defaultPiiReplacer() throws Exception {
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.setEnvironment(new MockEnvironment());
      context.register(PiiReplacerConfiguration.class);
      context.refresh();
      assertThat(context.getBean(PiiReplacer.class), is(instanceOf(DefaultPiiReplacer.class)));
    }
  }

  @Test
  public void customPiiReplacer() throws Exception {
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context.setEnvironment(
          new MockEnvironment().withProperty("piiReplacerClassName", IdentityPiiReplacer.class.getName()));
      context.register(PiiReplacerConfiguration.class);
      context.refresh();
      assertThat(context.getBean(PiiReplacer.class), is(instanceOf(IdentityPiiReplacer.class)));
    }
  }

  public static class IdentityPiiReplacer implements PiiReplacer {
    @Override
    public String replace(String value) {
      return value;
    }
  }
}
