# cloud-dataflow-samples-java

## dataflow

A few examples of streaming dataflow pipelines.


## Prerequisites

Install Java 7, and Maven 3.1.0 or higher. You may need to set your
JAVA_HOME.

## Compiling and Running from the Command Line

### To compile the examples:

You can compile the examples by running:

```
$ mvn clean compile bundle:bundle
```

### To run an injector locally:

Create a Pubsub topic:
https://cloud.google.com/pubsub/v1beta1/topics/create#try-it

Choose one or more of the following to run the corresponding injector:

```
$ java -cp target/examples-1.jar com.google.cloud.dataflow.examples.StockInjector /topics/$PROJECT/$TOPIC
$ java -cp target/examples-1.jar com.google.cloud.dataflow.examples.NewsInjector /topics/$PROJECT/$TOPIC
$ java -cp target/examples-1.jar com.google.cloud.dataflow.examples.WebSocketInjectorStub /topics/$PROJECT/$TOPIC

```


### To run an example pipeline:

Download the dataflow-streaming SDK, and copy the pipeline example to
cloud-dataflow/src/main/java/com/google/cloud/dataflow/examples/ directory in the SDK.

Compile the SDK:

```
$ mvn clean compile bundle:bundle
```


