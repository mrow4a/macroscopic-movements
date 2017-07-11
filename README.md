# PROJECT STRUCTURE:

- jobs: spark jobs which are dockerised with base image spark-submit

- server: user interface handling dockerised spark

- prototypes: all the source files used to write spark jobs

- documentation: project documentation

# HOW TO RUN:

Build spark jobs and create a docker image:

`./buildjob.sh`  

Build and run server:

`./run.sh`

# Hint

To run above commands, you would need SBT for jobs and Maven for server 
(It is done on purpose for educational purposes)

# Installing SBT

```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
sudo apt-get update
sudo apt-get install sbt
```

# Installing Maven

`sudo apt-get install maven`