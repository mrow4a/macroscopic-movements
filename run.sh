#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR/server && \
docker run -ti --rm -v $(pwd):/code -v "$HOME/.m2":/root/.m2 iflavoursbv/mvn-sbt-openjdk-8-alpine:latest mvn clean compile assembly:single && \
#mvn clean compile assembly:single && \
sudo chown -R $USER:$USER $DIR/server
java -jar $DIR/server/target/macromovements-server_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar
