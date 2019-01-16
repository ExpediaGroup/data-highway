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
package com.hotels.road.rest.model.validator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

public class RoadNameValidatorTest {
  private RoadNameValidator underTest;

  @Before
  public void setUp() {
    underTest = new RoadNameValidator();
  }

  @Test
  public void typical() {
    assertThat(underTest.validate("normal_road"), is("normal_road"));
  }

  @Test
  public void typicalWithNumber() {
    assertThat(underTest.validate("normal2"), is("normal2"));
  }

  @Test
  public void staticValidator() {
    assertThat(RoadNameValidator.validateRoadName("road"), is("road"));
  }

  @Test(expected = InvalidRoadNameException.class)
  public void staticValidatorInvokedWithInvalidRoadName() {
    RoadNameValidator.validateRoadName("2");
  }

  @Test(expected = InvalidRoadNameException.class)
  public void nullRoadName() {
    underTest.validate(null);
  }

  @Test(expected = InvalidRoadNameException.class)
  public void startsWithUnderscore() {
    underTest.validate("_road");
  }

  @Test(expected = InvalidRoadNameException.class)
  public void emptyString() {
    underTest.validate("");
  }

  @Test(expected = InvalidRoadNameException.class)
  public void whitespaceCharacters() {
    underTest.validate("  ");
  }

  @Test(expected = InvalidRoadNameException.class)
  public void startsWithNumber() {
    underTest.validate("2a");
  }

  @Test(expected = InvalidRoadNameException.class)
  public void startsWithACaptialLetter() {
    underTest.validate("Road");
  }

  @Test(expected = InvalidRoadNameException.class)
  public void capittalLettersInTheMiddle() {
    underTest.validate("wideRoad");
  }

  @Test(expected = InvalidRoadNameException.class)
  public void otherCharacters() {
    underTest.validate("road$");
  }
}
