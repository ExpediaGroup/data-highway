package com.hotels.road.s3.io;

import java.io.ByteArrayInputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3ConnectivityCheck {

  /**
   * Checking S3 connection with a put request.
   * This function throws a runtime exception if something goes wrong.
   *
   * @param s3
   * @param bucket
   * @param key
   */
  public void checkS3Put(AmazonS3 s3, String bucket, String key) {
    try {
      byte[] source = { 0x0 };
      ByteArrayInputStream is = new ByteArrayInputStream(source);

      final PutObjectRequest object = new PutObjectRequest(bucket, key, is, new ObjectMetadata());
      s3.putObject(object);
    } catch (Exception e) {
      log.error("Unable to write an object to AWS S3, bucket {}.", bucket);
      throw e;
    }
  }
}
