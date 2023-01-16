package org.example;

import java.util.List;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

/**
 * Hello world!
 */
public class SqsConsumer {

  public static void main(final String[] args) {
    validateInput(args);

    final var queueName = args[0];

    final var sqsClient = getSqsClient();
    final var queueUrlString = getQueueUrl(sqsClient, queueName);
    final var receivedMessages = receiveMessages(sqsClient, queueUrlString);

    printResults(receivedMessages);
    deleteProcessedMessages(sqsClient, receivedMessages, queueUrlString);

    sqsClient.close();
  }

  private static String getQueueUrl(final SqsClient client, final String queueName) {
    final var queueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();

    return client.getQueueUrl(queueUrlRequest).queueUrl();
  }

  private static void validateInput(String[] args) {
    if (args.length != 1) {
      System.out.println("Invalid program arguments!");
      System.out.println("Usage: <queue_name>");
      System.exit(1);
    }
  }

  private static SqsClient getSqsClient() {
    return SqsClient.builder()
        .region(Region.US_EAST_1)
        .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
        .build();
  }

  private static List<Message> receiveMessages(final SqsClient client,
      final String queueUrlString) {
    try {
      // if there are no messages in the queue - long polling, waiting for some to come
      SetQueueAttributesRequest queueAttributesRequest = SetQueueAttributesRequest.builder()
          .queueUrl(queueUrlString)
          .attributes(Map.of(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "20"))
          .build();

      client.setQueueAttributes(queueAttributesRequest);

      ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
          .queueUrl(queueUrlString)
          .maxNumberOfMessages(10)
          .visibilityTimeout(15)
          .waitTimeSeconds(20)
          .build();

      ReceiveMessageResponse response = client.receiveMessage(receiveMessageRequest);
      return response.messages();
    } catch (final SdkClientException exception) {
      System.err.println("Failed to receive the message");
      throw exception;
    }
  }

  private static void printResults(final List<Message> receivedMessages) {
    if (receivedMessages.isEmpty()) {
      System.out.println("No messages received!");
    } else {
      receivedMessages.stream()
          .map(Message::body)
          .forEach(
              receivedMessage -> System.out.println("Message '" + receivedMessage + "' received!"));
    }
  }

  private static void deleteProcessedMessages(final SqsClient client,
      final List<Message> receivedMessages, final String queueUrlString) {
    receivedMessages.stream()
        .map(Message::receiptHandle)
        .forEach(receipt -> {
          DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
              .queueUrl(queueUrlString)
              .receiptHandle(receipt)
              .build();
          client.deleteMessage(deleteMessageRequest);
        });
  }
}
