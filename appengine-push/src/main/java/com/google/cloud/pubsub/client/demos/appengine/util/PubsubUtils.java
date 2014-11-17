/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.google.cloud.pubsub.client.demos.appengine.util;

import com.google.api.client.extensions.appengine.http.UrlFetchTransport;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.extensions.appengine.auth.oauth2
        .AppIdentityCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.utils.SystemProperty;
import com.google.apphosting.api.ApiProxy;
import com.google.cloud.pubsub.client.demos.appengine.Constants;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Utility class to interact with the Pub/Sub API.
 */
public final class PubsubUtils {

    private static final String APPLICATION_NAME =
            "google-cloud-pubsub-appengine-sample/1.0";

    public static Pubsub getClient() throws IOException {
        HttpRequestInitializer credential;
        HttpTransport transport = new UrlFetchTransport();
        JsonFactory jsonFactory = new JacksonFactory();

        String serviceAccountEmail = System.getProperty(
                Constants.BASE_PACKAGE + ".serviceAccountEmail");
        String p12CertPath = System.getProperty(
                Constants.BASE_PACKAGE + ".p12certificatePath");

        if (SystemProperty.environment.value() ==
                SystemProperty.Environment.Value.Production) {
            credential = new AppIdentityCredential(PubsubScopes.all());
        } else {
            try {
                credential = new GoogleCredential.Builder()
                        .setJsonFactory(jsonFactory)
                        .setTransport(transport)
                        .setServiceAccountId(serviceAccountEmail)
                        .setServiceAccountPrivateKeyFromP12File(
                                new File(p12CertPath))
                        .setServiceAccountScopes(PubsubScopes.all())
                        .build();
            } catch (GeneralSecurityException e) {
                throw new IOException(
                        "Unable to generate a credential from a custom "
                                + "service account configuration.", e);
            }
        }

        Pubsub client = new Pubsub.Builder(transport, jsonFactory, credential)
                .setApplicationName(APPLICATION_NAME)
                .setHttpRequestInitializer(credential)
                .build();

        return client;
    }

    public static String getAppTopicName() {
        return "topic-pubsub-api-appengine-sample";
    }

    public static String getAppSubscriptionName() {
        return "subscription-" + getProjectId();
    }

    public static String getAppEndpointUrl() {
        String subscriptionUniqueToken = System.getProperty(
                Constants.BASE_PACKAGE + ".subscriptionUniqueToken");

        return "https://" + getProjectId() + ".appspot.com/receive_message" +
                "?token=" + subscriptionUniqueToken;
    }

    public static String getProjectId() {
        AppIdentityService identityService =
                AppIdentityServiceFactory.getAppIdentityService();

        // The project ID associated to an app engine application is the same
        // as the app ID.
        return identityService.parseFullAppId(ApiProxy.getCurrentEnvironment()
                .getAppId()).getId();
    }
}
