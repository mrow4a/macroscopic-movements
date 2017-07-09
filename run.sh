#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# Clean from last run
docker rm -f sparkjobserver
docker rm -f master
docker rm -f slave1
docker rm -f slave2
docker rm -f spark-datastore

#docker run --name sparkjobserver -d -p 4040:4040 -p 8090:8090 movements/spark-jobserver:latest
docker create -v /data --name spark-datastore brunocf/spark-datastore && \
docker run -d -p 8080:8080 -p 7077:7077 --volumes-from spark-datastore --name master brunocf/spark-master && \
docker run -d --link master:master --name slave1 --volumes-from spark-datastore brunocf/spark-slave && \
docker run -d --link master:master --name slave2 --volumes-from spark-datastore brunocf/spark-slave && \

# Build Movement Jobs
cd movements && \
#docker build -t movements/spark-jobserver:latest .
sbt package && \
mv target/scala-2.11/movements_2.11-0.1.0.jar $DIR/frontend/src/main/java/resources/movements.jar && \

cd $DIR/frontend && \
mvn clean compile assembly:single && \
java -jar $DIR/frontend/target/macromovements-frontend_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar


