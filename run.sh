#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR/server && \
mvn clean compile assembly:single && \
java -jar $DIR/server/target/macromovements-server_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar
