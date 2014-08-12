package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main class for the Cloud Pub/Sub command line sample application.
 */
public class Main {

    private static final String BOTNAME = "pubsub-irc-bot/1.0";

    private static final int PORT = 6667;

    private static Options options;

    static {
        options = new Options();
        options.addOption("noauth_local_webserver", false,
                "do not use a local webserver for authentication");

    }

    private static void checkArgsLength(String[] args, int min) {
        if (args.length < min) {
            help();
            System.exit(1);
        }
    }

    public static void help() {
        System.err.println("Usage: pubsub-sample.[sh|bat] [options] arguments");
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter writer = new PrintWriter(System.err);
        formatter.printOptions(writer, 80, options, 2, 2);
        writer.print("Available arguments are:\n"
                        + "PROJ list_topics\n"
                        + "PROJ create_topic TOPIC\n"
                        + "PROJ delete_topic TOPIC\n"
                        + "PROJ list_subscriptions\n"
                        + "PROJ create_subscription SUBSCRIPTION LINKED_TOPIC\n"
                        + "PROJ delete_subscription SUBSCRIPTION\n"
                        + "PROJ connect_irc TOPIC SERVER CHANNEL\n"
                        + "PROJ pull_messages SUBSCRIPTION\n"
        );
        writer.close();
    }

    public static void listTopics(Pubsub client, String[] args) throws IOException {
        Pubsub.Topics.List list = client.topics().list().setQuery(
                String.format("cloud.googleapis.com/project in (/projects/%s)", args[0]));
        String nextPageToken = null;
        do {
            if (nextPageToken != null) {
                list.setPageToken(nextPageToken);
            }
            ListTopicsResponse response = list.execute();
            for (Topic topic : response.getTopic()) {
                System.out.println(topic.getName());
            }
            nextPageToken = response.getNextPageToken();
        } while (nextPageToken != null);
    }

    public static void listSubscriptions(Pubsub client, String[] args) throws IOException {
        Pubsub.Subscriptions.List list = client.subscriptions().list().setQuery(
                String.format("cloud.googleapis.com/project in (/projects/%s)", args[0]));
        String nextPageToken = null;
        do {
            if (nextPageToken != null) {
                list.setPageToken(nextPageToken);
            }
            ListSubscriptionsResponse response = list.execute();
            for (Subscription subscription : response.getSubscription()) {
                System.out.println(subscription.toPrettyString());
            }
            nextPageToken = response.getNextPageToken();
        } while (nextPageToken != null);
    }

    public static void createTopic(Pubsub client, String[] args) throws IOException {
        checkArgsLength(args, 3);
        Topic topic = new Topic().setName(Utils.fqrn(Utils.ResourceType.TOPIC, args[0], args[2]));
        topic = client.topics().create(topic).execute();
        System.out.printf("Topic %s was created.\n", topic.getName());
    }

    public static void deleteTopic(Pubsub client, String[] args) throws IOException {
        checkArgsLength(args, 3);
        String topicName = Utils.fqrn(Utils.ResourceType.TOPIC, args[0], args[2]);
        client.topics().delete(topicName).execute();
        System.out.printf("Topic %s was deleted.\n", topicName);
    }

    public static void createSubscription(Pubsub client, String[] args) throws IOException {
        checkArgsLength(args, 4);
        Subscription subscription = new Subscription()
                .setTopic(Utils.fqrn(Utils.ResourceType.TOPIC, args[0], args[3]))
                .setName(Utils.fqrn(Utils.ResourceType.SUBSCRIPTION, args[0], args[2]));
        subscription = client.subscriptions().create(subscription).execute();
        System.out.printf("Subscription %s was created.\n", subscription.getName());
        System.out.println(subscription.toPrettyString());
    }

    public static void deleteSubscription(Pubsub client, String[] args) throws IOException {
        checkArgsLength(args, 3);
        String subscriptionName = Utils.fqrn(Utils.ResourceType.SUBSCRIPTION, args[0], args[2]);
        client.subscriptions().delete(subscriptionName).execute();
        System.out.printf("Subscription %s was deleted.\n", subscriptionName);
    }

    public static void connectIrc(Pubsub client, String[] args) throws IOException {
        checkArgsLength(args, 5);
        String server = args[3];
        String channel = args[4];
        String topic = Utils.fqrn(Utils.ResourceType.TOPIC, args[0], args[2]);
        String nick = String.format("bot-%s", args[0]);
        Socket socket = new Socket(server, PORT);
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream()));
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        writer.write(String.format("NICK %s\r\n", nick));
        writer.write(String.format("USER %s 8 * : %s\r\n", nick, BOTNAME));
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
                    String message = String.format("Title: %s, Diff: %s", m.group(1), m.group(2));
                    pubsubMessage.encodeData(message.getBytes("UTF-8"));
                } else {
                    pubsubMessage.encodeData(line.getBytes("UTF-8"));
                }
                PublishRequest publishRequest = new PublishRequest();
                publishRequest.setTopic(topic).setMessage(pubsubMessage);
                client.topics().publish(publishRequest).execute();
            }
        }
    }

    public static void pullMessages(Pubsub client, String[] args) throws IOException {
        checkArgsLength(args, 3);
        String subscriptionName = Utils.fqrn(Utils.ResourceType.SUBSCRIPTION, args[0], args[2]);
        PullRequest pullRequest = new PullRequest();
        pullRequest.setSubscription(subscriptionName);
        pullRequest.setReturnImmediately(false);

        while (true) {
            PullResponse pullResponse;
            try {
                pullResponse = client.subscriptions().pull(pullRequest).execute();
            } catch (Exception e) {
                // Something went wrong; wait a bit then try again.
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    System.exit(1);
                }
                continue;
            }
            PubsubMessage pubsubMessage = pullResponse.getPubsubEvent().getMessage();
            if (pubsubMessage != null) {
                System.out.println(new String(pubsubMessage.decodeData(), "UTF-8"));
                String id = pullResponse.getAckId();
                AcknowledgeRequest ackRequest = new AcknowledgeRequest();
                List<String> ackIds = new ArrayList<>(1);
                ackIds.add(id);
                ackRequest.setSubscription(subscriptionName).setAckId(ackIds);
                client.subscriptions().acknowledge(ackRequest).execute();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String[] cmdArgs = cmd.getArgs();
        checkArgsLength(cmdArgs, 2);
        Pubsub client = Utils.getClient(cmd.hasOption("noauth_local_webserver"));

        switch (cmdArgs[1]) {
            case "list_topics":
                listTopics(client, cmdArgs);
                break;
            case "create_topic":
                createTopic(client, cmdArgs);
                break;
            case "delete_topic":
                deleteTopic(client, cmdArgs);
                break;
            case "list_subscriptions":
                listSubscriptions(client, cmdArgs);
                break;
            case "create_subscription":
                createSubscription(client, cmdArgs);
                break;
            case "delete_subscription":
                deleteSubscription(client, cmdArgs);
                break;
            case "connect_irc":
                connectIrc(client, cmdArgs);
                break;
            case "pull_messages":
                pullMessages(client, cmdArgs);
                break;
            default:
                help();
                System.exit(1);
        }
    }
}
