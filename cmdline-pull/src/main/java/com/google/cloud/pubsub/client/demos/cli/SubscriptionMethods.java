package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullBatchRequest;
import com.google.api.services.pubsub.model.PullBatchResponse;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.Subscription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class SubscriptionMethods contains static methods for subscriptions.
 */
public class SubscriptionMethods {

    public static void createSubscription(Pubsub client, String[] args)
            throws IOException {
        Main.checkArgsLength(args, 4);
        Subscription subscription = new Subscription()
                .setTopic(Utils.getFullyQualifiedResourceName(
                        Utils.ResourceType.TOPIC, args[0], args[3]))
                .setName(Utils.getFullyQualifiedResourceName(
                        Utils.ResourceType.SUBSCRIPTION, args[0], args[2]));
        subscription = client.subscriptions().create(subscription).execute();
        System.out.printf(
                "Subscription %s was created.\n", subscription.getName());
        System.out.println(subscription.toPrettyString());
    }

    public static void pullMessages(Pubsub client, String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        String subscriptionName = Utils.getFullyQualifiedResourceName(
                Utils.ResourceType.SUBSCRIPTION, args[0], args[2]);
        PullBatchRequest pullBatchRequest = new PullBatchRequest()
                .setSubscription(subscriptionName)
                .setReturnImmediately(false)
                .setMaxEvents(Main.BATCH_SIZE);

        do {
            PullBatchResponse pullBatchResponse;
            pullBatchResponse = client.subscriptions()
                    .pullBatch(pullBatchRequest)
                    .execute();
            List<String> ackIds = new ArrayList<>(Main.BATCH_SIZE);
            List<PullResponse> pullResponses = pullBatchResponse
                    .getPullResponses();
            if (pullResponses != null) {
                for (PullResponse pullResponse : pullResponses) {
                    PubsubMessage pubsubMessage =
                            pullResponse.getPubsubEvent().getMessage();
                    if (pubsubMessage != null) {
                        System.out.println(
                                new String(pubsubMessage.decodeData(),
                                        "UTF-8"));
                        ackIds.add(pullResponse.getAckId());
                    }
                }
                AcknowledgeRequest ackRequest = new AcknowledgeRequest();
                ackRequest.setSubscription(subscriptionName).setAckId(ackIds);
                client.subscriptions().acknowledge(ackRequest).execute();
            }
        } while (System.getProperty(Main.LOOP_ENV_NAME) != null);
    }

    public static void listSubscriptions(Pubsub client, String[] args)
            throws IOException {
        Pubsub.Subscriptions.List list = client.subscriptions().list().setQuery(
                String.format("cloud.googleapis.com/project in (/projects/%s)",
                        args[0]));
        String nextPageToken = null;
        boolean hasSubscriptions = false;
        do {
            if (nextPageToken != null) {
                list.setPageToken(nextPageToken);
            }
            ListSubscriptionsResponse response = list.execute();
            if (!response.isEmpty()) {
                for (Subscription subscription : response.getSubscription()) {
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

    public static void deleteSubscription(Pubsub client, String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        String subscriptionName = Utils.getFullyQualifiedResourceName(
                Utils.ResourceType.SUBSCRIPTION, args[0], args[2]);
        client.subscriptions().delete(subscriptionName).execute();
        System.out.printf("Subscription %s was deleted.\n", subscriptionName);
    }
}
