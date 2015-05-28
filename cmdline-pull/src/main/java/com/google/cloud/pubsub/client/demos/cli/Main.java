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
public final class Main {

    /**
     * Prevents initialization.
     */
    private Main() {
    }

    /**
     * Pull batch size.
     */
    static final int BATCH_SIZE = 1000;

    /**
     * A name of environment variable for decide whether or not to loop.
     */
    static final String LOOP_ENV_NAME = "LOOP";

    /**
     * Options for parser.
     */
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
        /**
         * Action for creating a new topic.
         */
        create_topic {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                TopicMethods.createTopic(client, args);
            }
        },
        /**
         * Action for publishing a message to a topic.
         */
        publish_message {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                TopicMethods.publishMessage(client, args);
            }
        },
        /**
         * Action for connecting to an IRC channel and publishing messages to a
         * topic.
         */
        connect_irc {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                TopicMethods.connectIrc(client, args);
            }
        },
        /**
         * Action for listing topics in a project.
         */
        list_topics {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                TopicMethods.listTopics(client, args);
            }
        },
        /**
         * Action for deleting a topic.
         */
        delete_topic {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                TopicMethods.deleteTopic(client, args);
            }
        },
        /**
         * Action for creating a new subscription.
         */
        create_subscription {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                SubscriptionMethods.createSubscription(client, args);
            }
        },
        /**
         * Action for pulling messages from a subscription.
         */
        pull_messages {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                SubscriptionMethods.pullMessages(client, args);
            }
        },
        /**
         * Action for listing subscriptions in a project.
         */
        list_subscriptions {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                SubscriptionMethods.listSubscriptions(client, args);
            }
        },
        /**
         * Action for deleting a subscription.
         */
        delete_subscription {
            @Override
            void run(final Pubsub client, final String[] args)
                    throws IOException {
                SubscriptionMethods.deleteSubscription(client, args);
            }
        };

        /**
         * Abstruct method for this Enum.
         *
         * @param client Cloud Pub/Sub client.
         * @param args Command line arguments.
         * @throws IOException when Cloud Pub/Sub API calls fail.
         */
        abstract void run(Pubsub client, String[] args) throws IOException;
    }

    /**
     * Checks if the argument has enough length.
     *
     * @param args Command line arguments.
     * @param min Minimum length of the arguments.
     */
    static void checkArgsLength(final String[] args, final int min) {
        if (args.length < min) {
            help();
            System.exit(1);
        }
    }

    /**
     * Prints out the usage to stderr.
     */
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
                        + "PROJ create_subscription SUBSCRIPTION LINKED_TOPIC "
                        + "[PUSH_ENDPOINT]\n"
                        + "PROJ delete_subscription SUBSCRIPTION\n"
                        + "PROJ connect_irc TOPIC SERVER CHANNEL\n"
                        + "PROJ publish_message TOPIC MESSAGE\n"
                        + "PROJ pull_messages SUBSCRIPTION\n"
        );
        writer.close();
    }

    /**
     * Parses the command line arguments and calls a corresponding method.
     *
     * @param args Command line arguments.
     * @throws Exception when something bad happens.
     */
    public static void main(final String[] args) throws Exception {
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
