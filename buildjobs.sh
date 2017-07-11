#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# Build Movement Jobs
echo "Building jobs..." && \
cd jobs && \
#docker run -ti --rm -v $DIR/jobs:/code -v "$HOME/.ivy2":/root/.ivy2 iflavoursbv/mvn-sbt-openjdk-8-alpine:latest sbt clean package && \
sbt package && \
docker build -t "movements/jobs" $DIR/jobs