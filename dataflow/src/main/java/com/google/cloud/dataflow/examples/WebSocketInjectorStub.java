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

import com.google.api.services.pubsub.Pubsub;
// import com.google.api.services.pubsub.model.PublishBatchRequest;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.util.Transport;




// Imports for using WebSockets.
import org.glassfish.tyrus.client.ClientManager;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
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


// Adapted from https://blog.openshift.com/how-to-build-java-websocket-applications-using-the-jsr-356-api/
@ClientEndpoint
public class WebSocketInjectorStub {

  private static CountDownLatch latch;
  private Logger logger = Logger.getLogger(this.getClass().getName());

  private static String outputTopic;
  private Pubsub pubsub;
  private List<PubsubMessage> messages;

  private Integer batchSize;

  public WebSocketInjectorStub(Pubsub pubsub) {
    this.pubsub = pubsub;
    this.messages = new ArrayList<>(1);
    this.batchSize = new Integer(200);
  }

  @OnOpen
  public void onOpen(Session session) {
    logger.info("Connected ... " + session.getId());
    try {
      session.getBasicRemote().sendText("start");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @OnClose
  public void onClose(Session session, CloseReason closeReason) {
    logger.info(String.format("Session %s close because of %s", session.getId(),
                              closeReason));
    latch.countDown();
  }

  @OnMessage
  public void onMessage(String message, Session session) {
    //    logger.info("Received ...." + message);
    handleMessage(message);
  }

  public void handleMessage(String message) {
    System.out.println(">> message???");

    JsonObject jsonObject = Json.createReader(new StringReader(message)).readObject();

    String minorEdit = jsonObject.get("is_minor").toString();
    String pageTitle = jsonObject.getString("page_title");
    String pageURL = jsonObject.getString("url");
    String isBot = jsonObject.get("is_bot").toString();
    String isNew = jsonObject.get("is_new").toString();
    String user = jsonObject.getString("user");
    String isAnon = jsonObject.get("is_anon").toString();
    String changeSize = jsonObject.get("change_size").toString();

    String country;
      JsonObject geoIP = jsonObject.getJsonObject("geo_ip");
      if (geoIP == null) {
        country = new String("unknown country");
      } else {
        country = geoIP.getString("country_name");
      }

    String separator = new String("###");

    String finalOutput = minorEdit + separator + pageTitle + separator
      + pageURL + separator + isBot + separator + isNew + separator
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
            }

          }
        });
      thread.start();
    }

    /**/

  }


  /**
   * Command line parameter options.
   */
  private interface PubsubWebSocketInjectorOptions extends PipelineOptions {
    @Description("Topic to publish on.")
    @Validation.Required
    String getOutputTopic();
    void setOutputTopic(String value);
  }

  public static void main(String[] args) throws Exception {

    // Get options from command-line.
    PubsubWebSocketInjectorOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(PubsubWebSocketInjectorOptions.class);
    outputTopic = new String(options.getOutputTopic());


    // Create a Pubsub.
    StreamingOptions opts =
      options.as(StreamingOptions.class);
    Pubsub pubsub = Transport.newPubsubClient(opts).build();



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
