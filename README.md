# PROJECT STRUCTURE:

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

# HOW TO RUN:

All project is dockerised, and the only requirement is Docker. 

Spawn dockerised Spark Cluster and Minio S3:

`./preparedockers.sh`  

Build and run server docker containing all the jobs:

`./run.sh`
