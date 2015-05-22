package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.PushConfig;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class SubscriptionMethods contains static methods for subscriptions.
 */
public final class SubscriptionMethods {

    /**
     * Prevents instantiation.
     */
    private SubscriptionMethods() {
    }

    /**
     * Creates a new subscription.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Arguments as an array of String.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void createSubscription(final Pubsub client,
                                          final String[] args)
            throws IOException {
        Main.checkArgsLength(args, 4);
        String subscriptionName = PubsubUtils.getFullyQualifiedResourceName(
                PubsubUtils.ResourceType.SUBSCRIPTION, args[0], args[2]);
        Subscription subscription = new Subscription()
                .setTopic(PubsubUtils.getFullyQualifiedResourceName(
                        PubsubUtils.ResourceType.TOPIC, args[0], args[3]));
        if (args.length == 5) {
            subscription = subscription.setPushConfig(
                new PushConfig().setPushEndpoint(args[4]));
        }
        subscription = client.projects().subscriptions()
                .create(subscriptionName, subscription)
                .execute();
        System.out.printf(
                "Subscription %s was created.\n", subscription.getName());
        System.out.println(subscription.toPrettyString());
    }

    /**
     * Keeps pulling messages from the given subscription.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Arguments as an array of String.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void pullMessages(final Pubsub client, final String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        String subscriptionName = PubsubUtils.getFullyQualifiedResourceName(
                PubsubUtils.ResourceType.SUBSCRIPTION, args[0], args[2]);
        PullRequest pullRequest = new PullRequest()
                .setReturnImmediately(false)
                .setMaxMessages(Main.BATCH_SIZE);

        do {
            PullResponse pullResponse;
            pullResponse = client.projects().subscriptions()
                    .pull(subscriptionName, pullRequest)
                    .execute();
            List<String> ackIds = new ArrayList<>(Main.BATCH_SIZE);
            List<ReceivedMessage> receivedMessages =
                    pullResponse.getReceivedMessages();
            if (receivedMessages != null) {
                for (ReceivedMessage receivedMessage : receivedMessages) {
                    PubsubMessage pubsubMessage =
                            receivedMessage.getMessage();
                    if (pubsubMessage != null
                            && pubsubMessage.decodeData() != null) {
                        System.out.println(
                                new String(pubsubMessage.decodeData(),
                                        "UTF-8"));
                    }
                    ackIds.add(receivedMessage.getAckId());
                }
                AcknowledgeRequest ackRequest = new AcknowledgeRequest();
                ackRequest.setAckIds(ackIds);
                client.projects().subscriptions()
                        .acknowledge(subscriptionName, ackRequest)
                        .execute();
            }
        } while (System.getProperty(Main.LOOP_ENV_NAME) != null);
    }

    /**
     * Lists existing subscriptions within a project.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Arguments as an array of String.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void listSubscriptions(final Pubsub client,
                                         final String[] args)
            throws IOException {
        String nextPageToken = null;
        boolean hasSubscriptions = false;
        Pubsub.Projects.Subscriptions.List listMethod =
                client.projects().subscriptions().list("projects/" + args[0]);
        do {
            if (nextPageToken != null) {
                listMethod.setPageToken(nextPageToken);
            }
            ListSubscriptionsResponse response = listMethod.execute();
            if (!response.isEmpty()) {
                for (Subscription subscription : response.getSubscriptions()) {
                    hasSubscriptions = true;
                    System.out.println(subscription.toPrettyString());
                }
            }
            nextPageToken = response.getNextPageToken();
        } while (nextPageToken != null);
        if (!hasSubscriptions) {
            System.out.println(String.format(
                    "There are no subscriptions in the project '%s'.",
                    args[0]));
        }
    }

    /**
     * Deletes a subscription with a given name.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Arguments as an array of String.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void deleteSubscription(final Pubsub client,
                                          final String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        String subscriptionName = PubsubUtils.getFullyQualifiedResourceName(
                PubsubUtils.ResourceType.SUBSCRIPTION, args[0], args[2]);
        client.projects().subscriptions().delete(subscriptionName).execute();
        System.out.printf("Subscription %s was deleted.\n", subscriptionName);
    }
}
