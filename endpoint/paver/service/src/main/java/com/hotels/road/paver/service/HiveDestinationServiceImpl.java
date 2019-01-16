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
package com.hotels.road.paver.service;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.UnknownDestinationException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.HiveDestination;
import com.hotels.road.paver.api.HiveDestinationAdminClient;
import com.hotels.road.paver.service.exception.InvalidLandingIntervalException;
import com.hotels.road.rest.model.HiveDestinationModel;

@Component
public class HiveDestinationServiceImpl implements HiveDestinationService {

  private final HiveDestinationAdminClient hiveDestinationAdminClient;

  @Autowired
  public HiveDestinationServiceImpl(HiveDestinationAdminClient hiveDestinationAdminClient) {
    this.hiveDestinationAdminClient = hiveDestinationAdminClient;
  }

  @Override
  public HiveDestinationModel getHiveDestination(String name) throws UnknownRoadException, UnknownDestinationException {
    HiveDestination hiveDestination = hiveDestinationAdminClient.getHiveDestination(name).orElseThrow(
        () -> new UnknownDestinationException("Hive", name));

    HiveDestinationModel hiveDestinationModel = new HiveDestinationModel();
    hiveDestinationModel.setEnabled(hiveDestination.isEnabled());
    hiveDestinationModel.setLandingInterval(hiveDestination.getLandingInterval());
    return hiveDestinationModel;
  }

  @Override
  public void createHiveDestination(String name, HiveDestinationModel hiveDestinationModel)
    throws UnknownRoadException, AlreadyExistsException {
    if (hiveDestinationAdminClient.getHiveDestination(name).isPresent()) {
      throw new AlreadyExistsException(String.format("Hive destination for Road \"%s\" already exists.", name));
    }

    HiveDestination hiveDestination = new HiveDestination();
    hiveDestination.setLandingInterval(validateAndNormaliseLandingInterval(hiveDestinationModel.getLandingInterval()));
    hiveDestination.setEnabled(hiveDestinationModel.isEnabled());
    hiveDestinationAdminClient.createHiveDestination(name, hiveDestination);
  }

  @Override
  public void updateHiveDestination(String name, HiveDestinationModel hiveDestinationModel)
    throws UnknownRoadException, UnknownDestinationException {
    HiveDestination hiveDestination = hiveDestinationAdminClient.getHiveDestination(name).orElseThrow(
        () -> new UnknownDestinationException("Hive", name));

    hiveDestination.setLandingInterval(validateAndNormaliseLandingInterval(hiveDestinationModel.getLandingInterval()));
    hiveDestination.setEnabled(hiveDestinationModel.isEnabled());
    hiveDestinationAdminClient.updateHiveDestination(name, hiveDestination);
  }

  @VisibleForTesting
  String validateAndNormaliseLandingInterval(String landingInterval) {
    return Optional
        .ofNullable(landingInterval)
        .map(this::parseDuration)
        .map(this::validateIntervalRange)
        .map(Duration::toString)
        .orElse(HiveDestination.DEFAULT_LANDING_INTERVAL);
  }

  private Duration parseDuration(String duration) {
    try {
      return Duration.parse(duration);
    } catch (DateTimeParseException e) {
      throw new InvalidLandingIntervalException("Invalid landing interval.", e);
    }
  }

  private Duration validateIntervalRange(Duration duration) {
    if (duration.compareTo(HiveDestinationModel.MINIMUM_DURATION) == -1
        || duration.compareTo(HiveDestinationModel.MAXIMUM_DURATION) == 1) {
      throw new InvalidLandingIntervalException(
          String.format("Provided interval %s is outside of the required range of [%s, %s]", duration,
              HiveDestinationModel.MINIMUM_DURATION, HiveDestinationModel.MAXIMUM_DURATION));
    }
    return duration;
  }

}
