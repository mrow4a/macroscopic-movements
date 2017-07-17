### HOW TO RUN:

All project is dockerised, and the only requirement is Docker. 

Spawn dockerised Spark Cluster and Minio S3:

`./preparedockers.sh`  

Build and run server docker containing all the jobs:

`./run.sh`

### Overview

Project consists of three parts:

##### Stop Detection Algorithm

Stop Detection algorithm which given timestamp, latitude and longitude series is able
to determine which of detected points high be possible stay at the location.

##### DBSCAN on Spark

This is using an implementation of the [DBSCAN clustering algorithm](http://en.wikipedia.org/wiki/DBSCAN) 
on top of [Apache Spark](http://spark.apache.org/). It is loosely based on the paper from He, Yaobin, et al.
["MR-DBSCAN: a scalable MapReduce-based DBSCAN algorithm for heavily skewed data"](http://www.researchgate.net/profile/Yaobin_He/publication/260523383_MR-DBSCAN_a_scalable_MapReduce-based_DBSCAN_algorithm_for_heavily_skewed_data/links/0046353a1763ee2bdf000000.pdf). 

There is also a [visual guide](http://www.irvingc.com/visualizing-dbscan) that explains how the DBSCan algorithm works.

DBScan is used to cluster stops and detect most visited areas in which most of the locations been found

##### Graphx Graph Processing Engine

TODO: 



### PROJECT STRUCTURE:

<pre>

   smashbox
   ├── jobs/                          
   │   ├── clustering.dbscan      : Modified implementation of Irving's DBScan on Spark
   │   ├── stopdetection          : Stop detection algorithm implementation on Spark
   │   └── movements.jobs         : Spark-submit compatible jobs                      
   │
   ├── server/	                                 
   │   ├── ServerMain.java        : Entry Point of the Web Server                            
   │   ├── utils	              : Utilities for web server               
   │   ├── resources              : HTML, Javascript and CSS files               
   │   ├── api	                   
   │   │    ├── ApiHandler.java	  : Api Interface		
   │   │    └── SparkClient.java  : Api Implementation for Spark-submit	
   │   ├── sparkjobserver	      : <Not Used> Created for low-latency jobs, but not used in the project	
   │   └── movements.docker       : <Not Used> Creating as proof of concept for spawning dockers to hadle spark-submit                
   │
   ├── prototypes                 : Prototypes for testing of spark-submit jobs 		        
   │
   ├── spark-s3/                  : Dockerfile for mocking Spark Cluster             
   │
   ├── documentation/                                  
   │   └── smashbox/utilities                  
   │
   ├── preparedockers.sh          : Script to spawn Minio S3 
   │                                and Spark Master/Workers dockers       
   │   
   ├── run.sh                     : Script used to build the docker 
   │                                containing jobs and server
   │
   ├── Dockerfile                 : Dockerfile used to build server and jobs mobule,
   │                                configure and deploy the docker
   └── README                                   
   
</pre>

#### License

DBSCAN on Spark is available under the Apache 2.0 license. 
See the [LICENSE](LICENSE) file for details.


#### Credits

Stop Detection algorithm maintained by Piotr Mrowczynski, Gabriel Vilen and Ananya Chowdhury 
DBSCAN on Spark is maintained by Irving Cordova (irving@irvingc.com).

