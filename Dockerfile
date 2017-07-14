from csiebler/spark-s3

ENV TARGET_SERVER=/server/target/macromovements-server_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar
ENV TARGET_JOBS=/jobs/target/scala-2.11/movements_2.11-0.1.0.jar

ADD ${TARGET_SERVER} /server.jar
ADD ${TARGET_JOBS} /jobs.jar

EXPOSE 80

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
ENV SPARK_HOME /opt/spark

CMD ["java", "-jar", "/server.jar"]

