package org.example;

import java.util.List;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * Hello world!
 */
public class SqsConsumer {

  public static void main(final String[] args) {
    if (args.length != 1) {
      System.out.println("Invalid program arguments!");
      System.out.println("Usage: <queue_name>");
      System.exit(1);
    }

    final var queue = args[0];

    final var sqsClient = getSqsClient();
    final var receivedMessages = receiveMessage(sqsClient, queue);
    sqsClient.close();

    if (receivedMessages.isEmpty()) {
        System.out.println("No messages received!");
    } else {
        receivedMessages.forEach(
          receivedMessage -> System.out.println("Message '" + receivedMessage + "' received!"));
    }
  }

  private static SqsClient getSqsClient() {
    return SqsClient.builder()
        .region(Region.US_EAST_1)
        .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
        .build();
  }

  private static List<String> receiveMessage(final SqsClient client, final String queueName) {
    try {
      final var queueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();

      final var queueUrlString = client.getQueueUrl(queueUrlRequest).queueUrl();

      ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
          .queueUrl(queueUrlString)
          .maxNumberOfMessages(10)
          .visibilityTimeout(10)
          .build();

      ReceiveMessageResponse message = client.receiveMessage(receiveMessageRequest);
      return message.messages().stream()
          .map(Message::body)
          .toList();
    } catch (final SdkClientException exception) {
      System.err.println("Failed to receive the message");
      System.err.println(exception.getMessage());
      return List.of(
          "Exception occurred while trying to fetch the message: " + exception.getMessage());
    }
  }
}
