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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.cloud.pubsub.client.demos.appengine.Constants;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Fetches messages for display.
 */
public class FetchMessagesServlet extends HttpServlet {

    /**
     * Number of messages for a single fetch call.
     */
    private static final int MAX_COUNT = 20;

    @Override
    @SuppressWarnings("unchecked")
    public final void doGet(final HttpServletRequest req,
                            final HttpServletResponse resp)
            throws IOException {
        // First retrieve messages from the memcache
        MemcacheService memcacheService = MemcacheServiceFactory
                .getMemcacheService();
        List<String> messages =
                (List<String>) memcacheService.get(Constants.MESSAGE_CACHE_KEY);
        if (messages == null) {
            // If no messages in the memcache, look for the datastore
            DatastoreService datastore =
                    DatastoreServiceFactory.getDatastoreService();
            PreparedQuery query = datastore.prepare(
                    new Query("PubsubMessage").addSort("receipt-time",
                            Query.SortDirection.DESCENDING));
            messages = new ArrayList<>();
            for (Entity entity : query.asIterable(
                    FetchOptions.Builder.withLimit(MAX_COUNT))) {
                String message = (String) entity.getProperty("message");
                messages.add(message);
            }
            // Store them to the memcache for future use.
            memcacheService.put(Constants.MESSAGE_CACHE_KEY, messages);
        }
        ObjectMapper mapper = new ObjectMapper();
        resp.setContentType("application/json; charset=UTF-8");
        mapper.writeValue(resp.getWriter(), messages);
        resp.getWriter().close();
    }
}
