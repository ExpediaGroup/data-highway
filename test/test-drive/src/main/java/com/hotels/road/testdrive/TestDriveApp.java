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
package com.hotels.road.testdrive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.userdetails.User;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.security.RoadWebSecurityConfigurerAdapter;
import com.hotels.road.testdrive.MemoryRoadConsumer.StreamKey;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Slf4j
@EnableSwagger2
@SpringBootApplication(scanBasePackages = "com.hotels.road")
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class TestDriveApp {
  @Bean
  Map<String, Road> store() {
    return new HashMap<>();
  }

  @Bean
  Map<String, List<Record>> messages() {
    return new HashMap<>();
  }

  @Bean
  Map<StreamKey, AtomicInteger> commits() {
    return new HashMap<>();
  }

  @Bean
  MeterRegistry meterRegistry() {
    return new SimpleMeterRegistry();
  }

  @Bean
  public WebSecurityConfigurerAdapter webSecurityConfigurerAdapter() {
    return new RoadWebSecurityConfigurerAdapter() {
      @SuppressWarnings("deprecation")
      @Override
      protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.inMemoryAuthentication().withUser(
            User.withDefaultPasswordEncoder().username("user").password("pass").authorities("ROLE_USER"));
      }
    };
  }

  public static void main(String[] args) throws Exception {
    try {
      SpringApplication.run(TestDriveApp.class, args);
    } catch (Exception e) {
      log.error("Application failed.", e);
      System.exit(1);
    }
  }
}
