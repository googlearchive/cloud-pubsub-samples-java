package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Utility class for this sample application.
 */
public class Utils {

    private static final String APP_NAME = "cloud-pubsub-sample-cli/1.0";

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
        return String.format("projects/%s/%s/%s",
                project, resourceType.collectionName, resource);
    }

    /**
     * Builds a new Pubsub client and returns it.
     *
     * @return Pubsub client.
     * @throws IOException when we can not load the private key file.
     */
    public static Pubsub getClient(String serviceAccount, String secretFile)
            throws IOException, GeneralSecurityException {
        HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        GoogleCredential credential = new GoogleCredential.Builder()
                .setTransport(transport)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountScopes(PubsubScopes.all())
                .setServiceAccountId(serviceAccount)
                .setServiceAccountPrivateKeyFromP12File(new File(secretFile))
                .build();
        // Please use custom HttpRequestInitializer for automatic
        // retry upon failures.
        HttpRequestInitializer initializer =
                new RetryHttpInitializerWrapper(credential);
        return new Pubsub.Builder(transport, JSON_FACTORY, initializer)
                .setApplicationName(APP_NAME)
                .build();
    }
}
