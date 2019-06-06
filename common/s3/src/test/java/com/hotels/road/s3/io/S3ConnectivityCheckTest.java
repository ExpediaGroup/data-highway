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
package com.hotels.road.s3.io;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

@RunWith(SpringJUnit4ClassRunner.class)
public class S3ConnectivityCheckTest {

  private final S3ConnectivityCheck underTest = new S3ConnectivityCheck();

  @MockBean
  private AmazonS3 s3;

  @Test
  public void checkS3() {
    Mockito.when(
        s3.putObject(
            Mockito.any(PutObjectRequest.class)))
        .thenReturn(new PutObjectResult());

    underTest.checkS3Put(s3, "bucket", "key");
  }

  @Test(expected = SdkClientException.class)
  public void checkS3WithError() {
    Mockito.when(
        s3.putObject(
            Mockito.any(PutObjectRequest.class)))
        .thenThrow(new SdkClientException(""));

    underTest.checkS3Put(s3, "bucket", "key");
  }
}
