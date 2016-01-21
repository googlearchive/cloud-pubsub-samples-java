# Cloud Pub/Sub gRPC samples for Java

This is a sample repo for accessing Cloud Pub/Sub with
[gRPC](http://www.grpc.io/) client library.


## Prerequisites

Install Maven and Java7.

## Register your application and create a topic

- If you don't have a project, go to [Google Developers Console](https://console.developers.google.com/)
  and create a new project.

- Enable the "Google Cloud Pub/Sub" API under "APIs & auth > APIs."

- Go to "Credentials" and create a new Service Account,
  then download a new JSON file.

- Set the following environment variables.

  - GOOGLE_APPLICATION_CREDENTIALS: the file path to the downloaded JSON file.

- Create a topic if you haven't.

## Build the application

You need to fetch the git submodule first.

```
$ git submodule update --init
```

Then build the program.

```
$ mvn package
```

or

```
$ mvn compile
$ mvn assembly:single
```

## Run the application

```
$ bin/pubsub-sample.sh <your-project-id>
```

This program lists your topics in that project.
