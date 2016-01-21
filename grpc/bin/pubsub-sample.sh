#!/bin/bash

export SRC_DIR=$(cd "$(dirname "$0")/.."; pwd)
java -cp ${SRC_DIR}/target/grpc-sample-1.0-jar-with-dependencies.jar \
    com.google.cloud.pubsub.grpc.demos.Main "$@"
