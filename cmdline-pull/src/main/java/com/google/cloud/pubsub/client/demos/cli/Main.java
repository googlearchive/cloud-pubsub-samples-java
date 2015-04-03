package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.services.pubsub.Pubsub;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Main class for the Cloud Pub/Sub command line sample application.
 */
public class Main {

    static final int BATCH_SIZE = 1000;

    static final String LOOP_ENV_NAME = "LOOP";

    private static Options options;

    static {
        options = new Options();
        options.addOption("l", "loop", false,
                "Loop forever for pulling when specified");

    }

    /**
     * Enum representing subcommands.
     */
    private enum CmdLineOperation {
        create_topic {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                TopicMethods.createTopic(client, args);
            }
        }, publish_message {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                TopicMethods.publishMessage(client, args);
            }
        }, connect_irc {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                TopicMethods.connectIrc(client, args);
            }
        }, list_topics {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                TopicMethods.listTopics(client, args);
            }
        }, delete_topic {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                TopicMethods.deleteTopic(client, args);
            }
        }, create_subscription {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                SubscriptionMethods.createSubscription(client, args);
            }
        }, pull_messages {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                SubscriptionMethods.pullMessages(client, args);
            }
        }, list_subscriptions {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                SubscriptionMethods.listSubscriptions(client, args);
            }
        }, delete_subscription {
            @Override
            void run(Pubsub client, String[] args) throws IOException {
                SubscriptionMethods.deleteSubscription(client, args);
            }
        };
        abstract void run(Pubsub client, String[] args) throws IOException;
    }

    static void checkArgsLength(String[] args, int min) {
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
                        + "PROJ publish_message TOPIC MESSAGE\n"
                        + "PROJ pull_messages SUBSCRIPTION\n"
        );
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String[] cmdArgs = cmd.getArgs();
        checkArgsLength(cmdArgs, 2);
        if (cmd.hasOption("loop")) {
            System.setProperty(LOOP_ENV_NAME, "loop");
        }
        Pubsub client = PubsubUtils.getClient();
        try {
            CmdLineOperation cmdLineOperation =
                    CmdLineOperation.valueOf(cmdArgs[1]);
            cmdLineOperation.run(client, cmdArgs);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IllegalArgumentException e) {
            help();
            System.exit(1);
        }
    }
}
