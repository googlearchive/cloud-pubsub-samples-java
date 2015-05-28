package com.google.cloud.pubsub.client.demos.cli;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class PubsubIntegrationTest {

    private static final String DEFAULT_TEST_PROJECT_ID =
            "cloud-pubsub-sample-test";
    private static final String TEST_PROJECT_ID_ENV = "TEST_PROJECT_ID";
    private static final String PROJECT_ID;
    private static final String TOPIC_NAME;
    private static final String SUBSCRIPTION_NAME;

    private final ByteArrayOutputStream outContent =
            new ByteArrayOutputStream();
    private PrintStream originalOut;

    static {
        String projectId = System.getenv(TEST_PROJECT_ID_ENV);
        if (projectId == null) {
            PROJECT_ID = DEFAULT_TEST_PROJECT_ID;
        } else {
            PROJECT_ID = projectId;
        }
        UUID uuid = UUID.randomUUID();
        TOPIC_NAME = "test-topic-" + uuid;
        SUBSCRIPTION_NAME = "test-sub-" + uuid;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        String[] args = {PROJECT_ID, "create_topic", TOPIC_NAME};
        Main.main(args);
        String[] subArgs = {PROJECT_ID, "create_subscription",
                SUBSCRIPTION_NAME, TOPIC_NAME};
        Main.main(subArgs);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        String[] args = {PROJECT_ID, "delete_topic", TOPIC_NAME};
        Main.main(args);
        String[] subArgs = {PROJECT_ID, "delete_subscription",
                SUBSCRIPTION_NAME};
        Main.main(subArgs);
    }

    @Before
    public void swapStdout() throws Exception {
        originalOut = System.out;
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void restoreStdout() throws Exception {
        System.setOut(originalOut);
    }

    @Test
    public void testListTopic() throws Exception {
        String topicName = String.format("projects/%s/topics/%s",
                PROJECT_ID,
                TOPIC_NAME);
        String[] args = {PROJECT_ID, "list_topics"};
        Main.main(args);
        assertTrue(outContent.toString().contains(topicName));
    }

    @Test
    public void testPublishAndPullMessage() throws Exception {
        String message = "hello";
        String[] args = {PROJECT_ID, "publish_message", TOPIC_NAME, message};
        Main.main(args);
        assertThat(outContent.toString(),
                containsString("Published with a message id:"));
        outContent.reset();
        String[] pullArgs = {PROJECT_ID, "pull_messages", SUBSCRIPTION_NAME};
        Main.main(pullArgs);
        assertEquals(message, outContent.toString().trim());
    }
}
