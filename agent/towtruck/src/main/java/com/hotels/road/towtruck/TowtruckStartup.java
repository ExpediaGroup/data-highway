package com.hotels.road.towtruck;

import java.io.ByteArrayInputStream;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TowtruckStartup {

  @Autowired
  private AmazonS3 s3;
  @Value("${s3.bucket}")
  private String bucket;

  /**
   * Check connections before to start the application.
   * In case of a problem, we fail fast.
   */
  @PostConstruct
  public void postConstruct() {
    try {
      checkS3(s3, bucket);
    } catch (Exception e) {
      log.error("Application is going to be stopped.");
      throw e;
    }
  }

  /**
   * Test S3 connection
   *
   * @param s3
   */
  void checkS3(AmazonS3 s3, String bucket) {
    try {
      byte[] source = { 0x0 };
      ByteArrayInputStream is = new ByteArrayInputStream(source);

      final PutObjectRequest object = new PutObjectRequest(bucket, "test", is, new ObjectMetadata());
      s3.putObject(object);
    } catch (Exception e) {
      log.error("Unable to connect to Amazon S3.");
      throw e;
    }
  }
}
