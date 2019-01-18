package com.hotels.road.towtruck;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TowtruckStartup {

  @Autowired
  private AmazonS3 s3;

  /**
   * Check connections before to start the application.
   * In case of a problem, we fail fast.
   */
  @PostConstruct
  public void postConstruct() {
    try {
      testS3(s3);
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
  public void testS3(AmazonS3 s3) {
    try {
      s3.listBuckets();
    } catch (Exception e) {
      log.error("Unable to connect to Amazon S3.");
      throw e;
    }
  }
}
