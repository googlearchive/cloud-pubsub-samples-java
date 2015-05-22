package com.google.cloud.pubsub.client.demos.cli;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * Utility class for this sample application.
 */
public final class PubsubUtils {

    /**
     * The application name will be attached to the API requests.
     */
    private static final String APP_NAME = "cloud-pubsub-sample-cli/1.0";

    /**
     * Prevents instantiation.
     */
    private PubsubUtils() {
    }

    /**
     * Enum representing a resource type.
     */
    public enum ResourceType {
        /**
         * Represents topics.
         */
        TOPIC("topics"),
        /**
         * Represents subscriptions.
         */
        SUBSCRIPTION("subscriptions");
        /**
         * A path representation for the resource.
         */
        private String collectionName;
        /**
         * A constructor.
         *
         * @param collectionName String representation of the resource.
         */
        private ResourceType(final String collectionName) {
            this.collectionName = collectionName;
        }
        /**
         * Returns its collection name.
         *
         * @return the collection name.
         */
        public String getCollectionName() {
            return this.collectionName;
        }
    }

    /**
     * Returns the fully qualified resource name for Pub/Sub.
     *
     * @param resourceType ResourceType.
     * @param project A project id.
     * @param resource topic name or subscription name.
     * @return A string in a form of PROJECT_NAME/RESOURCE_NAME
     */
    public static String getFullyQualifiedResourceName(
            final ResourceType resourceType, final String project,
            final String resource) {
        return String.format("projects/%s/%s/%s", project,
                             resourceType.getCollectionName(), resource);
    }

    /**
     * Builds a new Pubsub client with default HttpTransport and
     * JsonFactory and returns it.
     *
     * @return Pubsub client.
     * @throws IOException when we can not get the default credentials.
     */
    public static Pubsub getClient() throws IOException {
        return getClient(Utils.getDefaultTransport(),
                         Utils.getDefaultJsonFactory());
    }

    /**
     * Builds a new Pubsub client and returns it.
     *
     * @param httpTransport HttpTransport for Pubsub client.
     * @param jsonFactory JsonFactory for Pubsub client.
     * @return Pubsub client.
     * @throws IOException when we can not get the default credentials.
     */
    public static Pubsub getClient(final HttpTransport httpTransport,
                                   final JsonFactory jsonFactory)
             throws IOException {
        Preconditions.checkNotNull(httpTransport);
        Preconditions.checkNotNull(jsonFactory);
        GoogleCredential credential =
            GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(PubsubScopes.all());
        }
        // Please use custom HttpRequestInitializer for automatic
        // retry upon failures.
        HttpRequestInitializer initializer =
            new RetryHttpInitializerWrapper(credential);
        return new Pubsub.Builder(httpTransport, jsonFactory, initializer)
                .setApplicationName(APP_NAME)
                .build();
    }
}
