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
package com.hotels.road.s3.io;

import static com.hotels.road.s3.io.S3MultipartOutputStream.MINIMUM_PART_SIZE;
import static com.hotels.road.s3.io.S3MultipartOutputStream.builder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.s3.AmazonS3;

@RunWith(MockitoJUnitRunner.class)
public class S3MultipartOutputStreamBuilderTest {

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  @Mock
  private AmazonS3 s3;

  @Test
  public void typical() {
    builder().s3(s3, BUCKET, KEY).partSize(MINIMUM_PART_SIZE).retries(1, 0).async(1, 1).build();
  }

  @Test(expected = NullPointerException.class)
  public void nullS3() {
    builder().s3(null, BUCKET, KEY).partSize(MINIMUM_PART_SIZE).retries(1, 0).async(1, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void blankBucket() {
    builder().s3(s3, " ", KEY).partSize(MINIMUM_PART_SIZE).retries(1, 0).async(1, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void blankKey() {
    builder().s3(s3, BUCKET, " ").partSize(MINIMUM_PART_SIZE).retries(1, 0).async(1, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void partSizeLessThanMinimum() {
    builder().s3(s3, BUCKET, KEY).partSize(MINIMUM_PART_SIZE - 1).retries(1, 0).async(1, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void maxAttemptsLessThanMinimum() {
    builder().s3(s3, BUCKET, KEY).partSize(MINIMUM_PART_SIZE - 1).retries(0, 0).async(1, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void sleepSecondsLessThanMinimum() {
    builder().s3(s3, BUCKET, KEY).partSize(MINIMUM_PART_SIZE - 1).retries(1, -1).async(1, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void poolSizeLessThanMinimum() {
    builder().s3(s3, BUCKET, KEY).partSize(MINIMUM_PART_SIZE - 1).retries(1, 0).async(0, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void queueSizeLessThanMinimum() {
    builder().s3(s3, BUCKET, KEY).partSize(MINIMUM_PART_SIZE - 1).retries(1, 0).async(1, 0).build();
  }

}
