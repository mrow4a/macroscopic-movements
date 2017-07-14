#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker rm -f spark-client

# Build server
cd $DIR/server && \
docker run -ti --rm -v $(pwd):/code -v "$HOME/.m2":/root/.m2 iflavoursbv/mvn-sbt-openjdk-8-alpine:latest mvn clean compile assembly:single && \

# Build jobs
cd $DIR/jobs && \
docker run -ti --rm -v $DIR/jobs:/code -v "$HOME/.ivy2":/root/.ivy2 iflavoursbv/mvn-sbt-openjdk-8-alpine:latest sbt clean package && \

# Create docker spark-client
docker build -t "movements/spark-client" $DIR
docker run --name spark-client -p 9999:80 "movements/spark-client"
