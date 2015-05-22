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

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

import org.joda.time.Duration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * A streaming injector for News sources using Pubsub I/O.
 *
 * <p> This pipeline example pulls top News stories from the web and publishes
 * them to two corresponding PubSub topics. </p>
 *
 * <p> To run this example using the Dataflow service, you must provide an
 * output pubsub topic for news, using the {@literal --inputTopic} option.
 * This injector can be run locally using the direct runner. </p>
 * E.g.:
 * java -cp target/examples-1.jar \
 *   com.google.cloud.dataflow.examples.StockInjector \
 *   --runner=DirectPipelineRunner \
 *   --project=google.com:clouddfe \
 *   --stagingLocation=gs://clouddfe-test/staging-$USER \
 *   --outputTopic=/topics/google.com:clouddfe/stocks1w1
 */


public class NewsInjector {

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

  private static String newsTopic;
  private Pubsub pubsub;

  private Logger logger = Logger.getLogger(this.getClass().getName());

  /**
   * Fetches the news from news.google.com and returns the titles.
   */
  public List<String> getNews() {
    // Get the top news stories.
    List<String> news = getNews("https://news.google.com/news?output=rss");
    // Get the top technology news stories.
    news.addAll(getNews(
        "https://news.google.com/news?cf=all&ned=us&hl=en&topic=tc&output=rss"));
    return news;
  }

  /**
   * Fetches the news from the specified URL and returns the titles.
   */
  public List<String> getNews(String newsUrl) {
    // Fetch news titles:
    List<String> newsTitles = new ArrayList<String>();
    try {
      String rssFeed = new String();
      URL feedSource = new URL(newsUrl);
      SyndFeedInput fi = new SyndFeedInput();
      SyndFeed feed = fi.build(new XmlReader(feedSource));
      for (Iterator i = feed.getEntries().iterator(); i.hasNext(); ) {
        SyndEntry entry = (SyndEntry) i.next();

        String title = entry.getTitle();
        title = title.substring(0, title.lastIndexOf("-"));

        String catPhrase = "  ";
        // Get the body of the news.
        String content = getContent(entry.getLink(), catPhrase);
        newsTitles.add(title + "###" + content);
      }
    } catch (MalformedURLException e) {
      ;
    } catch (IOException e) {
      ;
    } catch (FeedException e) {
      ;
    }
    return newsTitles;
  }

  /**
   * A constructor of NewsInjector.
   */
  public NewsInjector(Pubsub pubsub, String newsTopic) {
    this.pubsub = pubsub;
    this.newsTopic = newsTopic;
  }

  /**
   * Fetches the news titles and publishes them.
   */
  public void publishNews() {
    List<String> newsItems = getNews();
    for (String news : newsItems) {
      publishMessage(news, newsTopic);
    }
  }

  /**
   * Publishes the given message to the given topic.
   */
  public void publishMessage(String message, String outputTopic) {
    int maxLogMessageLength = 200;
    if (message.length() < maxLogMessageLength) {
      maxLogMessageLength = message.length();
    }
    logger.info("Received ...." + message.substring(0, maxLogMessageLength));

    // Publish message to Pubsub.
    PubsubMessage pubsubMessage = new PubsubMessage();
    pubsubMessage.encodeData(message.getBytes());

    PublishRequest publishRequest = new PublishRequest();
    publishRequest.setTopic(outputTopic).setMessage(pubsubMessage);
    try {
      this.pubsub.topics().publish(publishRequest).execute();
    } catch (java.io.IOException e) {
      logger.warning(e.getStackTrace().toString());
    }
  }

  /**
   * Retrieves the contents of the webpage.
   */
  public String getContent(String pageUrl, String catPhrase) {
    String content = new String();
    try {
      URL url = new URL(pageUrl);
      URLConnection conn = url.openConnection();
      BufferedReader br =
          new BufferedReader(new InputStreamReader(conn.getInputStream()));
      String inputLine;
      while ((inputLine = br.readLine()) != null) {
        content += catPhrase + inputLine;
      }
      br.close();
    } catch (MalformedURLException e) {
      ;
    } catch (IOException e) {
      ;
    }
    return content;
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
   * Fetches news and publishes them to the specified Cloud Pub/Sub topic.
   */
  public static void main(String[] args) throws Exception {
    // Get options from command-line.
    if (args.length < 1) {
      System.out.println("Please specify the output Pubsub topic.");
      return;
    }

    String newsTopic = new String(args[0]);

    System.out.println("Output Pubsub topic: " + newsTopic);

    NewsInjector injector = new NewsInjector(null, "");
    // Create a Pubsub.
    Pubsub client = injector.createPubsubClient();

    injector = new NewsInjector(client, newsTopic);

    while (true) {
      // Fetch news.
      injector.publishNews();

      try {
        //thread to sleep for the specified number of milliseconds
        Thread.sleep(20000);
      } catch ( java.lang.InterruptedException ie) {
        ;
      }
    }
  }
}
