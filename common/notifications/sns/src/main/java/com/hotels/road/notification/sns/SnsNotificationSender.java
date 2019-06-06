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
package com.hotels.road.notification.sns;

import lombok.extern.slf4j.Slf4j;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;

import com.hotels.road.notification.NotificationSender;
import com.hotels.road.notification.model.DataHighwayNotification;

@Slf4j
public class SnsNotificationSender implements NotificationSender {

  static final String ROAD_NAME = "roadName";
  static final String TYPE = "type";
  static final String PROTOCOL_VERSION = "protocolVersion";

  /** http://docs.aws.amazon.com/sns/latest/dg/large-payload-raw-message.html */
  private static final int SNS_MESSAGE_SIZE_LIMIT_BYTES = 256 * 1024;

  private final AmazonSNSAsync sns;

  private final TopicArnFactory topicArnFactory;

  private final MessageFactory messageFactory;

  private final ObjectWriter objectWriter;

  public SnsNotificationSender(AmazonSNSAsync sns, TopicArnFactory topicArnFactory, MessageFactory messageFactory) {
    log.info("Starting SNS notifier.");
    this.sns = sns;
    this.topicArnFactory = topicArnFactory;
    this.messageFactory = messageFactory;
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
    objectWriter = mapper.writer();
  }

  @Override
  public void send(DataHighwayNotification notification) {
    String topicArn = topicArnFactory.topicArn(notification);
    try {
      final String jsonMessage = objectWriter.writeValueAsString(notification);
      if (jsonMessage.getBytes(Charsets.UTF_8).length > SNS_MESSAGE_SIZE_LIMIT_BYTES) {
        log.error("Message length exceeds SNS limit ({} bytes).", SNS_MESSAGE_SIZE_LIMIT_BYTES);
      }
      log.debug("Attempting to send message to topic '{}': {}", topicArn, jsonMessage);
      String message = messageFactory.message(topicArn, notification);

      PublishRequest request = new PublishRequest(topicArn, jsonMessage, message);
      request.addMessageAttributesEntry(PROTOCOL_VERSION, attributeStringValue(notification.getProtocolVersion()));
      request.addMessageAttributesEntry(TYPE, attributeStringValue(notification.getType().name()));
      request.addMessageAttributesEntry(ROAD_NAME, attributeStringValue(notification.getRoadName()));

      try {
        sns.publish(request);
      } catch (AmazonClientException e) {
        log.error("Could not publish message '{}' to SNS topic '{}'.", message, topicArn, e);
      }
    } catch (JsonProcessingException e) {
      log.error("Could not serialize message '{}'.", notification, e);
    }

  }

  private MessageAttributeValue attributeStringValue(String value) {
    return new MessageAttributeValue().withDataType("String").withStringValue(value);
  }

  public interface TopicArnFactory {
    String topicArn(DataHighwayNotification notification);
  }

  public interface MessageFactory {
    String message(String topicArn, DataHighwayNotification notification);
  }

}
