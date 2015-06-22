package com.google.cloud.pubsub.client.demos.appengine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class IntegrationTest {

    private static final String DEFAULT_TEST_PROJECT_ID =
            "cloud-pubsub-sample-test";
    private static final String TEST_PROJECT_ID_ENV = "TEST_PROJECT_ID";
    private static final String PROJECT_ID;
    private static final String MESSAGE;
    private static final int MAX_RETRY = 3;
    private static final long SLEEP_TIME = 1000L;

    static {
        String projectId = System.getenv(TEST_PROJECT_ID_ENV);
        if (projectId == null) {
            PROJECT_ID = DEFAULT_TEST_PROJECT_ID;
        } else {
            PROJECT_ID = projectId;
        }
        UUID uuid = UUID.randomUUID();
        // The '=@~' part is for checking the descrepancy of base64
        // variants used on the server side and on the client side.
        MESSAGE = "=@~test-message-" + uuid;
    }

    private String getAppBaseURL() {
        return "https://" + PROJECT_ID + ".appspot.com/";
    }

    @Test
    public void testTopPage() throws Exception {
        String url = getAppBaseURL();
        URL obj = new URL(url);
        HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

        con.setRequestMethod("GET");

        int responseCode = con.getResponseCode();

        // It ensures that our Application Default Credentials work well.
        assertEquals(200, responseCode);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        String contents = response.toString();

        assertTrue(contents.contains(PROJECT_ID));
    }

    private String fetchMessages() throws Exception {
        String url = getAppBaseURL() + "fetch_messages";
        URL obj = new URL(url);
        HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

        con.setRequestMethod("GET");

        int responseCode = con.getResponseCode();

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        return response.toString();
    }

    @Test
    public void testSendMessage() throws Exception {
        String url = getAppBaseURL() + "send_message";
        URL obj = new URL(url);
        HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
        String body = "message=" + URLEncoder.encode(MESSAGE, "UTF-8");

        con.setRequestProperty(
                "Content-Type", "application/x-www-form-urlencoded");
        con.setRequestMethod("POST");

        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(body);
        wr.flush();
        wr.close();

        int responseCode = con.getResponseCode();

        // It ensures that the app successfully received the message.
        assertEquals(204, responseCode);

        // Try fetching the /fetch_messages endpoint and see if the
        // response contains the message.
        boolean found = false;
        for (int i = 0; i < MAX_RETRY; i++) {
            Thread.sleep(SLEEP_TIME);
            String resp = fetchMessages();
            if (resp.contains(MESSAGE)) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }
}
