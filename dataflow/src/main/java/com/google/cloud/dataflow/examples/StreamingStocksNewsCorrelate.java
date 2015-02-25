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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.RateLimiting;
import com.google.cloud.dataflow.sdk.transforms.SerializableComparator;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;

import org.joda.time.Duration;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Iterator;

import java.io.BufferedReader;
import java.io.InputStreamReader; 
import java.net.URL;
import java.net.URLConnection;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import com.sun.syndication.io.FeedException;
import java.net.MalformedURLException;


/**
 * A streaming Dataflow Example using Pubsub I/O, CoGroupByKey Join and BigQuery
 * output.
 *
 * <p> This pipeline example reads lines of text representing News articles and
 * stock information from two corresponding PubSub topics, parses each and
 * correlates them to provide a list of news stories that may affect each
 * companies' stock price.
 * The computed results are finally writen to a BigQuery table. </p>
 *
 * <p> To run this example using the Dataflow service, you must provide input
 * pubsub topics for stock and news, and an output BigQuery table, using the
 * {@literal --inputTopic} {@literal --dataset} and {@literal --table} options.
 * Since this is a streaming pipeline that never completes, select the
 * non-blocking pipeline runner
 * {@literal --runner=DataflowPipelineRunner}.
 */


public class StreamingStocksNewsCorrelate {


  // Converts strings into BigQuery rows.
  static class StringToRowConverter extends DoFn<KV<String, String>, TableRow> {

    // In this example, put the whole string into single BigQuery field.
    @Override
    public void processElement(ProcessContext c) {
      String outputstring = "News: " + c.element().getKey()
        + "   :  " + c.element().getValue();
      c.output(new TableRow().set("string_field", outputstring));
    }

    static TableSchema getSchema() {
      return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
        // Compose the list of TableFieldSchema from tableSchema.
        {
          add(new TableFieldSchema().setName("string_field").setType("STRING"));
        }
      });
    }
  }


  /**
   * Extracts the stock title and price change from a stocks stream.
   */
  static class ExtractStockInfoFn
    extends DoFn<String, KV<String, String>> {
    @Override
    public void processElement(ProcessContext c) {
      String[] x = c.element().split("###");
      if (x.length >= 2) {
        String title = x[0].toLowerCase();
        String priceChange = x[1];
        if (!title.isEmpty() && !priceChange.isEmpty())
          c.output(KV.of(title, priceChange));
      }
    }
  }


  /**
   * Extracts the news title and relevant words from a news stream.
   * Output: {@code KV<Word, NewsTitle>}.
   */
  static class ExtractNewsInfoFn
    extends DoFn<String, KV<String, String>> {

    List<String> stopWords =
      new ArrayList<String>(Arrays.asList("is", "an", "a", "the", "as", "and",
                                          "to", "for", "After", "by", "on",
                                          "in", "can", "will", "be"));

    @Override
    public void processElement(ProcessContext c) {
      String[] x = c.element().split("###");
      if (x.length >= 2) {
        String title = x[0];
        // Output all words in news title.
        List<String> wordsInPage =
          new ArrayList<String>(
            Arrays.asList(x[0].toLowerCase().split("\\W+")));
        wordsInPage.removeAll(stopWords);
        for (String word : wordsInPage) {
          if (!word.isEmpty())
            c.output(KV.of(word, title));
        }
      }
    }
  }



  /**
   * Join two collections, using company name as the key.
   */
  static PCollection<KV<String, String>>
    correlateEvents(PCollection<KV<String, String>> stocks,
                    PCollection<KV<String, String>> inverseNews) {

    final TupleTag<String> stockInfoTag = new TupleTag<>();
    final TupleTag<String> newsInfoTag = new TupleTag<>();

    // transform both input collections to tuple collections, where the keys are company
    // names in both cases.
    // company name 'key' -> CGBKR (<stock price>, <news title>)
    PCollection<KV<String, CoGbkResult>> kvpCollection =
      KeyedPCollectionTuple
        .of(stockInfoTag, stocks)
        .and(newsInfoTag, inverseNews)
        .apply(CoGroupByKey.<String>create());

    // Process the CoGbkResult elements generated by the CoGroupByKey transform.
    // company name 'key' -> string of <news title>, <company name>
    PCollection<KV<String, String>> finalResultCollection =
      kvpCollection.apply(ParDo.of(
        new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
        @Override
        public void processElement(ProcessContext c) {
           KV<String, CoGbkResult> e = c.element();
           CoGbkResult val = e.getValue();
           String companyName = e.getKey();

           Double stockPrice = 0.0;
           int cnt = 0;
           for (String stockPriceStr :
                  c.element().getValue().getAll(stockInfoTag)) {
             stockPrice += Double.parseDouble(stockPriceStr);
             ++cnt;
           }
           stockPrice /= cnt;

           // Remove duplicate news items.
           Set<String> allNewsInfoSet = new HashSet<String>();
           for (String newsInfo : c.element().getValue().getAll(newsInfoTag)) {
             allNewsInfoSet.add(newsInfo);
           }
           String allNewsInfo = "";
           for (String newsInfo : allNewsInfoSet) {
             allNewsInfo += newsInfo + ", ";
           }
           // Only output when there is news, and for a real stock.
           if (!companyName.isEmpty() && !allNewsInfo.isEmpty() &&
               !Double.isNaN(stockPrice)) {
             c.output(KV.of(companyName + " (" + stockPrice + ")", " {" +
                            allNewsInfo + "}"));
           }
        }
      }));

    // // write to GCS: combines all stocks related to a news story.
    // // key: String(news title), value: String({list stocks affected by key}).
    // PCollection<KV<String, String>> correlatedStocks = finalResultCollection
    //   .apply(Combine.<String, String>perKey(
    //     new SerializableFunction<Iterable<String>, String>(){
    //       @Override
    //       public String apply(Iterable<String> values) {
    //         String output = "";
    //         for (String value : values) {
    //           output += value + ", ";
    //         }
    //         return " {" + output + "}";
    //       }
    //     }));
    // return correlatedStocks;
    return finalResultCollection;
  }



  // Command line parameter options.
  private interface StreamingStocksNewsCorrelateOptions
    extends PipelineOptions {
    @Description("Input Pubsub topic")
    @Validation.Required
    String getInputTopic();
    void setInputTopic(String value);

    @Description("BigQuery dataset name")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("BigQuery table name")
    @Validation.Required
    String getTable();
    void setTable(String value);
  }


  // Sets up and starts streaming pipeline.
  public static void main(String[] args) {
    StreamingStocksNewsCorrelateOptions options =
      PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(StreamingStocksNewsCorrelateOptions.class);
    DataflowPipelineOptions dataflowOptions =
      options.as(DataflowPipelineOptions.class);
    dataflowOptions.setStreaming(true);
    options.as(StreamingOptions.class).setStreaming(true);
    Pipeline pipeline = Pipeline.create(options);

    String tableSpec = new StringBuilder()
        .append(dataflowOptions.getProject()).append(":")
        .append(options.getDataset()).append(".")
        .append(options.getTable())
        .toString();

    // Stocks holds the entire stocks information: <Title, change>
    PCollection<KV<String, String>> stocks = pipeline
      .apply(PubsubIO.Read.topic(options.getInputTopic()))
      .apply(ParDo.named("extractStockInfo")
             .of(new ExtractStockInfoFn())) // Extract the Stock information
      .apply(Window.<KV<String, String>>into(
               FixedWindows.of(Duration.standardSeconds(300)))); // Windowing.

    // news holds the entire news information: <Title, {"words", "in", "text"}>
    PCollection<KV<String, String>> news = pipeline
      .apply(PubsubIO.Read.topic(options.getInputTopic()+"_news"))
      .apply(ParDo.named("extractNewsInfo")
             .of(new ExtractNewsInfoFn())) // Extract the News information.
      .apply(Window.<KV<String, String>>into(
               FixedWindows.of(Duration.standardSeconds(300)))); // Windowing.

    // Match stocks to News titles and output.
    correlateEvents(stocks, news)
      .apply(ParDo.of(new StringToRowConverter()))
      .apply(BigQueryIO.Write.to(tableSpec)
             .withSchema(StringToRowConverter.getSchema()));

    pipeline.run();
  }
}
