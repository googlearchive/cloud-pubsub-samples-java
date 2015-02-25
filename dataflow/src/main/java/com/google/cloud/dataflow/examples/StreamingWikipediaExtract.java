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
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.RateLimiting;
import com.google.cloud.dataflow.sdk.transforms.SerializableComparator;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.joda.time.Duration;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A streaming Dataflow Example using Pubsub I/O and BigQuery output.
 *
 * <p> This pipeline example reads lines of text representing Wikipedia edits
 * from a PubSub topic, parses each line to create an object with the details
 * of the edits, processes the edits list to compute the following:
 * 1) Edit activity per Country/User/Page
 * 2) Top-k Edits per Country/User/Page
 * 3) Flaged activity that exceeds a threshold
 * The computed results are finally writen to Pubsub Topics (for 1 and 3), and
 * to a BigQuery table (for 2). </p>
 *
 * <p> To run this example using the Dataflow service, you must provide an input
 * pubsub topic, an output pubsub topic, and an output BigQuery table, using the
 * {@literal --inputTopic} {@literal --outputTopic} {@literal --dataset} and
 * {@literal --table} options. Since this is a streaming pipeline that never
 * completes, select the non-blocking pipeline runner
 * {@literal --runner=DataflowPipelineRunner}.
 */


public class StreamingWikipediaExtract {

  /**
   * A class to hold structured Wikipedia edit information.
   *
   * The strean must contain the following fields:
   * {"is_minor": false,
   * "page_title": "Template:Citation needed/testcases",
   * "url": "http://en.wikipedia.org/w/index.php?diff=553804313&oldid=479472901",
   * "is_unpatrolled": false,
   * "is_bot": false,
   * "is_new": false,
   * // "summary": null,
   * // "flags": null,
   * "user": "98.172.160.184",
   * "country": "India",
   * "is_anon": true,
   * // "ns": "Template",
   * "change_size": "+42"}
   */
  static class WikiInfo implements Serializable {
    Boolean minorEdit;
    String pageTitle;
    String pageURL;
    Boolean isBot;
    Boolean isNew;
    String user;
    String country;
    Boolean isAnon;
    Integer changeSize;

    WikiInfo(Boolean minorEdit, String pageTitle, String pageURL, Boolean isBot,
             Boolean isNew, String user, String country, Boolean isAnon,
             Integer changeSize) {
      this.minorEdit = minorEdit;
      this.pageTitle = pageTitle;
      this.pageURL = pageURL;
      this.isBot = isBot;
      this.isNew = isNew;
      this.user = user;
      this.country = country;
      this.isAnon = isAnon;
      this.changeSize = changeSize;
    }

    String getPageTitle() {
      return new String("Page: " + pageTitle);
    }

    String getUser() {
      return new String("User: " + user);
    }

    String getCountry() {
      return new String("Country: " + country);
    }
  };

  /** A DoFn that tokenizes lines of text into individual words. */
  static class ExtractWikiInfo extends DoFn<String, WikiInfo> {
    @Override
    public void processElement(ProcessContext c) {
      if (!c.element().isEmpty()) {
        String[] words = c.element().split("###");  // assumes the input stream is "###" separated.
      
        c.output(new WikiInfo(new Boolean(Boolean.parseBoolean(words[0])), // Minor edit.
                              words[1],                                    // Page Title.
                              words[2],                                    // Page URL.
                              new Boolean(Boolean.parseBoolean(words[3])), // isBot.
                              new Boolean(Boolean.parseBoolean(words[4])), // isNew.
                              words[5],                                    // User.
                              words[6],                                    // Country.
                              new Boolean(Boolean.parseBoolean(words[7])), // isAnon.
                              new Integer(Integer.parseInt(words[8]))      // change size.
                              ));
      }
    }
  }


  // Converts strings into BigQuery rows.
  static class StringToRowConverter extends DoFn<String, TableRow> {

    // In this example, put the whole string into single BigQuery field.
    @Override
    public void processElement(ProcessContext c) {
      c.output(new TableRow().set("string_field", c.element()));
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
   * {@code PTransform} for computing statistics from a
   * {@code PCollection<String>} of Wikipedia-edit metadata.
   *
   * Computes the edit-rate per entry, the top-k most active edits,
   * and high-activity edits in the given time-period. Output is
   * returned as a {@code PCollectionTuple} with three
   * {@code PCollection<String>}s.
   */
  static class ComputeStatistics
    extends PTransform<PCollection<String>, PCollectionTuple> {

    private Integer topK;
    private Integer threshold;

    private String editRatesTag;
    private String topEditsTag;
    private String flagedEditsTag;

    public static ComputeStatistics create(Integer topK, Integer threshold,
                                           String editRatesTag,
                                           String topEditsTag,
                                           String flagedEditsTag) {
      return new ComputeStatistics(topK, threshold,
                                   editRatesTag, topEditsTag, flagedEditsTag);
    }

    private ComputeStatistics(Integer topK, Integer threshold,
                              String editRatesTag,
                              String topEditsTag,
                              String flagedEditsTag) {
      this.topK = topK;
      this.threshold = threshold;
      this.editRatesTag = editRatesTag;
      this.topEditsTag = topEditsTag;
      this.flagedEditsTag = flagedEditsTag;
    }

    @Override
    public PCollectionTuple apply(PCollection<String> tag) {

      PCollection<KV<String, Long>> tagCounts =
        tag.apply(Count.<String>perElement());

      // Report edit rates per user/page/country.
      PCollection<String> editRatesPerTag =
        tagCounts.apply(ParDo.named("activeEditCounts")
                        .of(new DoFn<KV<String, Long>, String>() {
                            @Override
                            public void processElement(ProcessContext c) {
                              c.output(c.element().getKey());
                            }
                          }));


      // Compute most active edit users/pages/countries in a time period
      PCollection<String> topTags =
        tagCounts.apply(Top.of(topK, new SerializableComparator<KV<String, Long>>() {
              @Override
              public int compare(KV<String, Long> o1, KV<String, Long> o2) {
                return Long.compare(o1.getValue(), o2.getValue());
              }
            }))
        .apply(ParDo.of(new DoFn<List<KV<String, Long>>, String>() {
              @Override
              public void processElement(ProcessContext c) {
                for (KV<String, Long> item : c.element()) {
                  if (item.getValue() > 1) {  // Only report for non-trivial values.
                    c.output(new String(item.getKey() + " : " + item.getValue()));
                  }
                }
              }
            }));


      // Flag high-activity edits users/pages/countries in a time period, beyond threshold.
      PCollection<String> highActivityTags =
        tagCounts.apply(ParDo.named("flagHighActivityEdits")
                        .of(new DoFn<KV<String, Long>, String>() {
                            @Override
                            public void processElement(ProcessContext c) {
                              if (c.element().getValue() > threshold) {
                                c.output(c.element().getKey());
                              }
                            }
                        }));

      // Create a tuple to return to the user, with specified tags.
      PCollectionTuple pcs =
        PCollectionTuple.of(new TupleTag<String>(editRatesTag), editRatesPerTag)
        .and(new TupleTag<String>(topEditsTag), topTags)
        .and(new TupleTag<String>(flagedEditsTag), highActivityTags);
      return pcs;
    }
  }


  /**
   * {@code PTransform} for outputing computed Wikipedia statistics
   * stored as a {@code PCollectionTuple}.
   *
   * Outputs the first and third {@code PCollection<String>}s to their
   * own Pubsub topics, and the second {@code PCollection<String>} to
   * a {@code BigQuery} table.
   */
  static class OutputWikiStats
    extends PTransform<PCollectionTuple, PDone> {

    private String tableSpec;
    private String activeEditsOutputTopic;
    private String flagedEditsOutputTopic;

    private String editRatesTag;
    private String topEditsTag;
    private String flagedEditsTag;

    public static OutputWikiStats create(String tableSpec,
                                         String activeEditsOutputTopic,
                                         String flagedEditsOutputTopic,
                                         String editRatesTag,
                                         String topEditsTag,
                                         String flagedEditsTag) {
      return new OutputWikiStats(tableSpec, activeEditsOutputTopic, flagedEditsOutputTopic,
                                 editRatesTag, topEditsTag, flagedEditsTag);
    }

    private OutputWikiStats(String tableSpec,
                            String activeEditsOutputTopic,
                            String flagedEditsOutputTopic,
                            String editRatesTag,
                            String topEditsTag,
                            String flagedEditsTag) {
      this.tableSpec = tableSpec;
      this.activeEditsOutputTopic = activeEditsOutputTopic;
      this.flagedEditsOutputTopic = flagedEditsOutputTopic;
      this.editRatesTag = editRatesTag;
      this.topEditsTag = topEditsTag;
      this.flagedEditsTag = flagedEditsTag;
    }

    @Override
    public PDone apply(PCollectionTuple pcs) {

      // Output edit rates to PubSub Topic.
      pcs.get(new TupleTag<String>(editRatesTag))
        .apply(PubsubIO.Write.topic(activeEditsOutputTopic));

      // Output topK edits to the BigQuery Table.
      pcs.get(new TupleTag<String>(topEditsTag))
        .apply(ParDo.of(new StringToRowConverter()))
        .apply(BigQueryIO.Write.to(tableSpec)
               .withSchema(StringToRowConverter.getSchema()));

      // Output high-activity flaged edits to PubSub Topic.
      pcs.get(new TupleTag<String>(flagedEditsTag))
        .apply(PubsubIO.Write.topic(flagedEditsOutputTopic));

      return new PDone();
    }
  }


  // Command line parameter options.
  private interface StreamingWikipediaExtractOptions extends PipelineOptions {
    @Description("Input Pubsub topic")
    @Validation.Required
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Topic to publish on for active edits.")
    @Validation.Required
    String getOutputTopic();
    void setOutputTopic(String value);

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
    StreamingWikipediaExtractOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(StreamingWikipediaExtractOptions.class);
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setStreaming(true);
    options.as(StreamingOptions.class).setStreaming(true);
    Pipeline pipeline = Pipeline.create(options);

    String tableSpec = new StringBuilder()
        .append(dataflowOptions.getProject()).append(":")
        .append(options.getDataset()).append(".")
        .append(options.getTable())
        .toString();

    // edits holds the entire edit information. 
    PCollection<WikiInfo> edits = pipeline
      .apply(PubsubIO.Read.topic(options.getInputTopic()))
      .apply(ParDo.of(new ExtractWikiInfo())) // Extract the Wiki information
      .apply(Window.<WikiInfo>into(FixedWindows.of(Duration.standardSeconds(60)))); // for windowing

    // Extract the country and return as string
    PCollection<String> countries = // ExtractCountryInfo
      edits.apply(ParDo.of(new DoFn<WikiInfo, String>() {
                        @Override
                          public void processElement(ProcessContext c) {
                            c.output(c.element().getCountry());
                        }
                      }));
    // Extract the users and return as string
    PCollection<String> users = // ExtractUserInfo
      edits.apply(ParDo.of(new DoFn<WikiInfo, String>() {
                        @Override
                          public void processElement(ProcessContext c) {
                            c.output(c.element().getUser());
                        }
                      }));
    // Extract the pages and return as string
    PCollection<String> pages =   // ExtractPageInfo
      edits.apply(ParDo.of(new DoFn<WikiInfo, String>() {
                        @Override
                          public void processElement(ProcessContext c) {
                            c.output(c.element().getPageTitle());
                        }
                      }));

    // Threshold for selecting the top-k edit rankings.
    Integer topK = new Integer(5);
    // Threshold for triggering high activity edits.
    Integer threshold = new Integer(10);

    // Tags for identifying PCollections within a PCollectionTuple.
    String tag1 = new String("editRates");
    String tag2 = new String("topEdits");
    String tag3 = new String("flagedEdits");


    // Compute the statistics for countries, users, and pages.
    PCollectionTuple countryStats =
      countries.apply(ComputeStatistics.create(topK, threshold, tag1, tag2, tag3));
    PCollectionTuple userStats =
      users.apply(ComputeStatistics.create(topK, threshold, tag1, tag2, tag3));
    PCollectionTuple pageStats =
      pages.apply(ComputeStatistics.create(topK, threshold, tag1, tag2, tag3));


    // Output the statistics for countries, users, and pages.
    countryStats.apply(OutputWikiStats.create(tableSpec, options.getOutputTopic(),
                                              options.getOutputTopic() + "_flaged",
                                              tag1, tag2, tag3));
    userStats.apply(OutputWikiStats.create(tableSpec, options.getOutputTopic(),
                                           options.getOutputTopic() + "_flaged",
                                           tag1, tag2, tag3));
    pageStats.apply(OutputWikiStats.create(tableSpec, options.getOutputTopic(),
                                           options.getOutputTopic() + "_flaged",
                                           tag1, tag2, tag3));

    pipeline.run();
  }
}
