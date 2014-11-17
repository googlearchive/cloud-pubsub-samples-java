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

package com.google.cloud.pubsub.client.demos.appengine.servlet;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PushConfig;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.cloud.pubsub.client.demos.appengine.util.PubsubUtils;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Entry point that initializes the application Pub/Sub resources.
 */
public class InitServlet extends HttpServlet {

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        Pubsub client = PubsubUtils.getClient();
        String projectId = PubsubUtils.getProjectId();
        req.setAttribute("project", projectId);

        setupTopic(client);
        setupSubscription(client);

        req.setAttribute("topic", PubsubUtils.getAppTopicName());
        req.setAttribute("subscription", PubsubUtils.getAppSubscriptionName());
        req.setAttribute("subscriptionEndpoint",
                PubsubUtils.getAppEndpointUrl()
                        .replaceAll("token=[^&]*", "token=REDACTED"));
        RequestDispatcher rd = req.getRequestDispatcher("/WEB-INF/main.jsp");
        try {
            rd.forward(req, resp);
        } catch (ServletException e) {
            throw new IOException(e);
        }
    }

    private final void setupTopic(Pubsub client)
            throws IOException {
        String fullName = String.format("/topics/%s/%s",
                PubsubUtils.getProjectId(),
                PubsubUtils.getAppTopicName());

        try {
            client.topics().get(fullName).execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                // Create the topic if it doesn't exist
                Topic topic = new Topic().setName(fullName);
                client.topics().create(topic).execute();
            } else {
                throw e;
            }
        }
    }

    private final void setupSubscription(Pubsub client)
            throws IOException {
        String fullName = String.format("/subscriptions/%s/%s",
                PubsubUtils.getProjectId(),
                PubsubUtils.getAppSubscriptionName());

        try {
            client.subscriptions().get(fullName).execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                // Create the subscription if it doesn't exist
                String fullTopicName = String.format("/topics/%s/%s",
                        PubsubUtils.getProjectId(),
                        PubsubUtils.getAppTopicName());
                PushConfig pushConfig = new PushConfig()
                        .setPushEndpoint(PubsubUtils.getAppEndpointUrl());
                Subscription subscription = new Subscription()
                        .setTopic(fullTopicName)
                        .setName(fullName)
                        .setPushConfig(pushConfig);
                client.subscriptions().create(subscription).execute();
            } else {
                throw e;
            }
        }
    }
}
