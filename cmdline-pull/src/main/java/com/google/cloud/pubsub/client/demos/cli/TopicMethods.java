package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishBatchRequest;
import com.google.api.services.pubsub.model.PublishBatchResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class TopicMethods contains static methods for topics.
 */
public class TopicMethods {

    private static final String BOT_NAME = "pubsub-irc-bot/1.0";

    private static final int PORT = 6667;

    public static void createTopic(Pubsub client, String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        Topic topic = new Topic().setName(
                Utils.getFullyQualifiedResourceName(Utils.ResourceType.TOPIC,
                        args[0], args[2]));
        topic = client.topics().create(topic).execute();
        System.out.printf("Topic %s was created.\n", topic.getName());
    }

    public static void connectIrc(Pubsub client, String[] args)
            throws IOException {
        Main.checkArgsLength(args, 5);
        String server = args[3];
        String channel = args[4];
        String topic = Utils.getFullyQualifiedResourceName(
                Utils.ResourceType.TOPIC, args[0], args[2]);
        String nick = String.format("bot-%s", args[0]);
        Socket socket = new Socket(server, PORT);
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream()));
        BufferedReader reader = new BufferedReader(
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
        Pattern p = Pattern.compile(
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
                int i = line.indexOf(privmsgMark);
                if (i == -1) { continue; }
                line = line.substring(i + privmsgMark.length(), line.length());
                PubsubMessage pubsubMessage = new PubsubMessage();
                Matcher m = p.matcher(line);
                if (m.find()) {
                    String message = String.format("Title: %s, Diff: %s",
                            m.group(1), m.group(2));
                    pubsubMessage.encodeData(message.getBytes("UTF-8"));
                } else {
                    pubsubMessage.encodeData(line.getBytes("UTF-8"));
                }
                List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
                PublishBatchRequest publishBatchRequest =
                        new PublishBatchRequest();
                publishBatchRequest.setTopic(topic).setMessages(messages);
                client.topics().publishBatch(publishBatchRequest).execute();
            }
        }
    }

    public static void publishMessage(Pubsub client, String[] args)
            throws IOException {
        Main.checkArgsLength(args, 4);
        String topic = Utils.getFullyQualifiedResourceName(
                Utils.ResourceType.TOPIC, args[0], args[2]);
        String message = args[3];
        PubsubMessage pubsubMessage = new PubsubMessage()
                .encodeData(message.getBytes("UTF-8"));
        List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
        PublishBatchRequest publishBatchRequest =
                new PublishBatchRequest();
        publishBatchRequest.setTopic(topic).setMessages(messages);
        PublishBatchResponse publishBatchResponse =
                client.topics().publishBatch(publishBatchRequest).execute();
        List<String> messageIds = publishBatchResponse.getMessageIds();
        if (messageIds != null) {
            for (String messageId : messageIds) {
                System.out.println("Published with a message id: " + messageId);
            }
        }
    }

    public static void deleteTopic(Pubsub client, String[] args)
            throws IOException {
        Main.checkArgsLength(args, 3);
        String topicName = Utils.getFullyQualifiedResourceName(
                Utils.ResourceType.TOPIC, args[0], args[2]);
        client.topics().delete(topicName).execute();
        System.out.printf("Topic %s was deleted.\n", topicName);
    }

    public static void listTopics(Pubsub client, String[] args)
            throws IOException {
        Pubsub.Topics.List list = client.topics().list().setQuery(
                String.format("cloud.googleapis.com/project in (/projects/%s)",
                        args[0]));
        String nextPageToken = null;
        boolean hasTopics = false;
        do {
            if (nextPageToken != null) {
                list.setPageToken(nextPageToken);
            }
            ListTopicsResponse response = list.execute();
            if (!response.isEmpty()) {
                for (Topic topic : response.getTopic()) {
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
