/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;


import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.glassfish.tyrus.client.ClientManager;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonObject;
import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.DeploymentException;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;


@ClientEndpoint
public class WebSocketInjectorStub {

  class RetryHttpInitializerWrapper implements HttpRequestInitializer {

    private Logger logger =
        Logger.getLogger(RetryHttpInitializerWrapper.class.getName());

    // Intercepts the request for filling in the "Authorization"
    // header field, as well as recovering from certain unsuccessful
    // error codes wherein the Credential must refresh its token for a
    // retry.
    private final GoogleCredential wrappedCredential;

    // A sleeper; you can replace it with a mock in your test.
    private final Sleeper sleeper;

    public RetryHttpInitializerWrapper(GoogleCredential wrappedCredential) {
      this(wrappedCredential, Sleeper.DEFAULT);
    }

    // Use only for testing.
    RetryHttpInitializerWrapper(
            GoogleCredential wrappedCredential, Sleeper sleeper) {
      this.wrappedCredential = Preconditions.checkNotNull(wrappedCredential);
      this.sleeper = sleeper;
    }

    @Override
    public void initialize(HttpRequest request) {
      final HttpUnsuccessfulResponseHandler backoffHandler =
          new HttpBackOffUnsuccessfulResponseHandler(
              new ExponentialBackOff())
          .setSleeper(sleeper);
      request.setInterceptor(wrappedCredential);
      request.setUnsuccessfulResponseHandler(
          new HttpUnsuccessfulResponseHandler() {
              @Override
              public boolean handleResponse(HttpRequest request,
                                            HttpResponse response,
                                            boolean supportsRetry)
                  throws IOException {
                if (wrappedCredential.handleResponse(request,
                                                     response,
                                                     supportsRetry)) {
                  // If credential decides it can handle it, the
                  // return code or message indicated something
                  // specific to authentication, and no backoff is
                  // desired.
                  return true;
                } else if (backoffHandler.handleResponse(request,
                                                         response,
                                                         supportsRetry)) {
                  // Otherwise, we defer to the judgement of our
                  // internal backoff handler.
                  logger.info("Retrying " + request.getUrl());
                  return true;
                } else {
                  return false;
                }
              }
          });
      request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(
          new ExponentialBackOff()).setSleeper(sleeper));
    }
  }

  private static CountDownLatch latch;
  private Logger logger = Logger.getLogger(this.getClass().getName());

  private static String outputTopic;
  private Pubsub pubsub;
  private List<PubsubMessage> messages;

  private Integer batchSize;

  /**
   * A constructor of WebSocketInjectorStub.
   */
  public WebSocketInjectorStub(Pubsub pubsub) {
    this.pubsub = pubsub;
    this.messages = new ArrayList<>(1);
    this.batchSize = new Integer(200);
  }

  /**
   * A callback when created a new websocket connection.
   */
  @OnOpen
  public void onOpen(Session session) {
    logger.info("Connected ... " + session.getId());
    try {
      session.getBasicRemote().sendText("start");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A callback when a websocket connection is disconnected.
   */
  @OnClose
  public void onClose(Session session, CloseReason closeReason) {
    logger.info(String.format("Session %s close because of %s", session.getId(),
                              closeReason));
    latch.countDown();
  }

  /**
   * A callback when a message comes through a websocket connection.
   */
  @OnMessage
  public void onMessage(String message, Session session) {
    //    logger.info("Received ...." + message);
    handleMessage(message);
  }

  /**
   * Parses the message and publishes the formatted string to a Cloud Pub/Sub topic.
   */
  public void handleMessage(String message) {
    System.out.println(">> message???");

    JsonObject jsonObject = Json.createReader(new StringReader(message)).readObject();

    String minorEdit = jsonObject.get("is_minor").toString();
    String pageTitle = jsonObject.getString("page_title");
    String pageUrl = jsonObject.getString("url");
    String isBot = jsonObject.get("is_bot").toString();
    String isNew = jsonObject.get("is_new").toString();
    String user = jsonObject.getString("user");
    String isAnon = jsonObject.get("is_anon").toString();
    String changeSize = jsonObject.get("change_size").toString();

    String country;
    JsonObject geoIp = jsonObject.getJsonObject("geo_ip");
    if (geoIp == null) {
      country = new String("unknown country");
    } else {
      country = geoIp.getString("country_name");
    }

    String separator = new String("###");

    String finalOutput = minorEdit + separator + pageTitle + separator
        + pageUrl + separator + isBot + separator + isNew + separator
        + user + separator + country + separator + isAnon + separator + changeSize;

    if (finalOutput.isEmpty()) {
      return;
    }
    logger.info("Received ...." + finalOutput);

    // Publish message to Pubsub.
    PubsubMessage pubsubMessage = new PubsubMessage();

    try {
      pubsubMessage.encodeData(finalOutput.getBytes("UTF-8"));
    } catch (java.io.UnsupportedEncodingException e) {
      ;
    }

    /*
    messages.add(pubsubMessage);

    if (messages.size() > batchSize) {
      PublishBatchRequest publishBatchRequest = new PublishBatchRequest()
        .setTopic(outputTopic)
        .setMessages(messages);
        pubsub.topics().publishBatch(publishBatchRequest).execute();
        messages.clear();
    }
    */
    /**/
    final PublishRequest publishRequest = new PublishRequest();

    publishRequest.setTopic(outputTopic).setMessage(pubsubMessage);
    // pubsub.topics().publish(publishRequest).execute();

    for (int i = 0; i < 20; ++i) {
      // publish on a new thread.
      Thread thread = new Thread(new Runnable() {
          public void run() {
            try {
              pubsub.topics().publish(publishRequest).execute();
            } catch (java.io.IOException e) {
              ;
            }
          }
        });
      thread.start();
    }

    /**/

  }


  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /**
   * Creates a Cloud Pub/Sub client.
   */
  public Pubsub createPubsubClient()
    throws IOException, GeneralSecurityException {
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    HttpRequestInitializer initializer =
        new RetryHttpInitializerWrapper(credential);
    return new Pubsub.Builder(transport, JSON_FACTORY, initializer).build();
  }

  /**
   * Creates a new websocket connection to a URL and publishes the
   * messages received from the connection.
   */
  public static void main(String[] args) throws Exception {

    // Get options from command-line.
    if (args.length < 1) {
      System.out.println("Please specify the output Pubsub topic.");
      return;
    }

    String outputTopic = new String(args[0]);

    System.out.println("Output Pubsub topic: " + outputTopic);

    WebSocketInjectorStub injector = new WebSocketInjectorStub(null);
    // Create a Pubsub.
    Pubsub pubsub = injector.createPubsubClient();

    latch = new CountDownLatch(1);
 
    ClientManager client = ClientManager.createClient();

    try {
      client.connectToServer(new WebSocketInjectorStub(pubsub),
                             new URI("ws://wikimon.hatnote.com"));
      latch.await();
 
    } catch (DeploymentException | URISyntaxException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
