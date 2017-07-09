#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Clean from last run
docker rm -f sparkjobserver
docker rm -f master

#docker run --name sparkjobserver -d -p 4040:4040 -p 8090:8090 movements/spark-jobserver:latest && \
#docker build -t movements/spark-jobserver:latest .

cd $DIR/frontend && \
mvn clean compile assembly:single && \
java -jar $DIR/frontend/target/macromovements-frontend_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar
