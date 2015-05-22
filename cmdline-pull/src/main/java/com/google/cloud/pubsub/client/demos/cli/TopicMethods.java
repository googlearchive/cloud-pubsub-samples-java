package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class TopicMethods contains static methods for topics.
 */
public final class TopicMethods {

    /**
     * Prevents initialization.
     */
    private TopicMethods() {
    }

    /**
     * IRC bot name.
     */
    private static final String BOT_NAME = "pubsub-irc-bot/1.0";

    /**
     * IRC port number.
     */
    private static final int PORT = 6667;

    /**
     * Creates a new topic with a given name.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Command line arguments.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void createTopic(final Pubsub client, final String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        String topicName = PubsubUtils.getFullyQualifiedResourceName(
                PubsubUtils.ResourceType.TOPIC, args[0], args[2]);
        Topic topic = client.projects().topics()
                .create(topicName, new Topic())
                .execute();
        System.out.printf("Topic %s was created.\n", topic.getName());
    }

    /**
     * Connects an IRC channel and publish the chat messages to the given topic.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Command line arguments.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void connectIrc(final Pubsub client, final String[] args)
            throws IOException {
        Main.checkArgsLength(args, 5);
        final String server = args[3];
        final String channel = args[4];
        final String topic = PubsubUtils.getFullyQualifiedResourceName(
                PubsubUtils.ResourceType.TOPIC, args[0], args[2]);
        final String nick = String.format("bot-%s", args[0]);
        final Socket socket = new Socket(server, PORT);
        final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream()));
        final BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));

        writer.write(String.format("NICK %s\r\n", nick));
        writer.write(String.format("USER %s 8 * : %s\r\n", nick, BOT_NAME));
        writer.flush();

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.contains("004")) {
                System.out.printf("Connected to %s.\n", server);
                break;
            } else if (line.contains("433")) {
                System.err.println("Nickname is already in use.");
                return;
            }
        }

        writer.write(String.format("JOIN %s\r\n", channel));
        writer.flush();

        // A regex pattern for Wikipedia change log as of June 4, 2014
        Pattern pat = Pattern.compile(
                "\\u000314\\[\\[\\u000307(.*)\\u000314\\]\\]\\u0003.*"
                        + "\\u000302(http://[^\\u0003]*)\\u0003");
        while ((line = reader.readLine()) != null) {
            if (line.toLowerCase().startsWith("PING ")) {
                // We must respond to PINGs to avoid being disconnected.
                writer.write("PONG " + line.substring(5) + "\r\n");
                writer.write("PRIVMSG " + channel + " :I got pinged!\r\n");
                writer.flush();
            } else {
                String privmsgMark = "PRIVMSG " + channel + " :";
                int prividx = line.indexOf(privmsgMark);
                if (prividx == -1) {
                    continue;
                }
                line = line.substring(prividx + privmsgMark.length(),
                                      line.length());
                PubsubMessage pubsubMessage = new PubsubMessage();
                Matcher matcher = pat.matcher(line);
                if (matcher.find()) {
                    String message = String.format("Title: %s, Diff: %s",
                            matcher.group(1), matcher.group(2));
                    pubsubMessage.encodeData(message.getBytes("UTF-8"));
                } else {
                    pubsubMessage.encodeData(line.getBytes("UTF-8"));
                }
                List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
                PublishRequest publishRequest = new PublishRequest();
                publishRequest.setMessages(messages);
                client.projects().topics()
                        .publish(topic, publishRequest)
                        .execute();
            }
        }
    }

    /**
     * Publishes the given message to the given topic.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Command line arguments.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void publishMessage(final Pubsub client, final String[] args)
            throws IOException {
        Main.checkArgsLength(args, 4);
        String topic = PubsubUtils.getFullyQualifiedResourceName(
                PubsubUtils.ResourceType.TOPIC, args[0], args[2]);
        String message = args[3];
        PubsubMessage pubsubMessage = new PubsubMessage()
                .encodeData(message.getBytes("UTF-8"));
        List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
        PublishRequest publishRequest = new PublishRequest();
        publishRequest.setMessages(messages);
        PublishResponse publishResponse = client.projects().topics()
                .publish(topic, publishRequest)
                .execute();
        List<String> messageIds = publishResponse.getMessageIds();
        if (messageIds != null) {
            for (String messageId : messageIds) {
                System.out.println("Published with a message id: " + messageId);
            }
        }
    }

    /**
     * Deletes a topic with the given name.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Command line arguments.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void deleteTopic(final Pubsub client, final String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        String topicName = PubsubUtils.getFullyQualifiedResourceName(
                PubsubUtils.ResourceType.TOPIC, args[0], args[2]);
        client.projects().topics().delete(topicName).execute();
        System.out.printf("Topic %s was deleted.\n", topicName);
    }

    /**
     * Lists existing topics in the project.
     *
     * @param client Cloud Pub/Sub client.
     * @param args Command line arguments.
     * @throws IOException when Cloud Pub/Sub API calls fail.
     */
    public static void listTopics(final Pubsub client, final String[] args)
            throws IOException {
        String nextPageToken = null;
        boolean hasTopics = false;
        Pubsub.Projects.Topics.List listMethod =
                client.projects().topics().list("projects/" + args[0]);
        do {
            if (nextPageToken != null) {
                listMethod.setPageToken(nextPageToken);
            }
            ListTopicsResponse response = listMethod.execute();
            if (!response.isEmpty()) {
                for (Topic topic : response.getTopics()) {
                    hasTopics = true;
                    System.out.println(topic.getName());
                }
            }
            nextPageToken = response.getNextPageToken();
        } while (nextPageToken != null);
        if (!hasTopics) {
            System.out.println(String.format(
                    "There are no topics in the project '%s'.", args[0]));
        }
    }
}
