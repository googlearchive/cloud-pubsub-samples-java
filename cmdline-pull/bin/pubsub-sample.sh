#!/bin/bash

export SRC_DIR=$(cd "$(dirname "$0")/.."; pwd)
java -cp ${SRC_DIR}/target/pubsub-pull-sample-1.0-jar-with-dependencies.jar \
  com.google.cloud.pubsub.client.demos.cli.Main "$@"
