#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

docker rm -f sparkjobserver
docker run --name sparkjobserver -d -p 8090:8090 sparkjobserver/spark-jobserver:0.7.0.mesos-0.25.0.spark-1.6.2

# Build Stop Detection Jobs
cd stopdetection
sbt package
mv target/scala-2.11/stopdetection_2.11-0.1.0.jar $DIR/frontend/src/main/java/resources/stopdetection.jar

cd $DIR/frontend
mvn clean compile assembly:single
java -jar $DIR/frontend/target/macromovements-frontend_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar


