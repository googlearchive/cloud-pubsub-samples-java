package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2
        .AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.extensions.java6.auth.oauth2
        .GooglePromptReceiver;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Utility class for this sample application.
 */
public class Utils {

    private static final String APP_NAME = "cloud-pubsub-sample-cli/1.0";

    private static final File CREDENTIALS_DIR =
        new File(System.getProperty("user.home"), ".pubsub-sample/secret");

    private static FileDataStoreFactory datastoreFactory;

    private static HttpTransport httpTransport;

    private static final JsonFactory JSON_FACTORY =
            JacksonFactory.getDefaultInstance();

    /**
     * Enum representing a resource type.
     */
    public enum ResourceType {
        TOPIC("topics"),
        SUBSCRIPTION("subscriptions");
        public String collectionName;
        private ResourceType(String collectionName) {
            this.collectionName = collectionName;
        }
    }
    /**
     * Returns the fully qualified resource name for Pub/Sub.
     *
     * @param resource topic name or subscription name
     * @return A string in a form of PROJECT_NAME/RESOURCE_NAME
     */
    public static String getFullyQualifiedResourceName(
            ResourceType resourceType, String project, String resource) {
        return String.format("/%s/%s/%s",
                resourceType.collectionName, project, resource);
    }

    private static Credential authorize(boolean noAuthLocalWebServer)
            throws Exception {
        // load client secrets
        InputStream inputStream =
                Utils.class.getResourceAsStream("/client_secrets.json");
        if (inputStream == null) {
            System.err.println("Download a json file containing "
                               + "Client ID and Secret from "
                               + "https://console.developers.google.com/ into "
                               + "src/main/resources/client_secrets.json");
            System.exit(1);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
            JSON_FACTORY, new InputStreamReader(inputStream));
        // set up authorization code flow
        GoogleAuthorizationCodeFlow flow =
                new GoogleAuthorizationCodeFlow.Builder(
                        httpTransport, JSON_FACTORY, clientSecrets,
                        PubsubScopes.all())
                        .setDataStoreFactory(datastoreFactory)
                        .build();
        // authorize
        if (noAuthLocalWebServer) {
            return new AuthorizationCodeInstalledApp(
                    flow, new GooglePromptReceiver()).authorize("user");
        } else {
            return new AuthorizationCodeInstalledApp(
                    flow, new LocalServerReceiver()).authorize("user");
        }
    }

    /**
     * Builds a new Pubsub client and returns it.
     *
     * @return Pubsub client.
     * @throws IOException when we can not load the private key file.
     */
    public static Pubsub getClient(boolean noAuthLocalWebServer)
            throws Exception {
        datastoreFactory = new FileDataStoreFactory(CREDENTIALS_DIR);
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        Credential credential = authorize(noAuthLocalWebServer);
        return new Pubsub.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APP_NAME)
                .build();
    }
}
