package com.hotels.road.towtruck;

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
public class TowtruckStartupTest {

  private final TowtruckStartup underTest = new TowtruckStartup();

  @MockBean
  private AmazonS3 s3;

  @Test
  public void checkS3() {
    Mockito.when(
        s3.putObject(
            Mockito.any(PutObjectRequest.class)))
        .thenReturn(new PutObjectResult());

    underTest.checkS3(s3, "bucket");
  }

  @Test(expected = SdkClientException.class)
  public void checkS3WithError() {
    Mockito.when(
        s3.putObject(
            Mockito.any(PutObjectRequest.class)))
        .thenThrow(new SdkClientException(""));

    underTest.checkS3(s3, "bucket");
  }
}
