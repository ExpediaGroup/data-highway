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
package com.hotels.road.truck.park;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.logging.LoggingSystem;

import lombok.extern.slf4j.Slf4j;

import com.hotels.road.boot.DataHighwayApplication;

@SpringBootApplication
@Slf4j
public class TruckParkApp {
  @Autowired
  private LoggingSystem loggingSystem;

  @PreDestroy
  public void shutdownLoggingSystem() {
    log.info("About to shutdown TruckParkApp logging system {}.", loggingSystem);
    loggingSystem.getShutdownHandler().run();
  }

  public static void main(String[] args) {
    DataHighwayApplication.run(TruckParkApp.class, args);
  }
}
