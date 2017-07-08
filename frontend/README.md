# Macroscopic Movements

### Overview

Project consists of three parts:

##### Stop Detection Algorithm

Stop Detection algorithm which given timestamp, latitude and longitude series is able
to determine which of detected points high be possible stay at the location.

##### DBScan on Spark

This is using an implementation of the [DBSCAN clustering algorithm](http://en.wikipedia.org/wiki/DBSCAN) 
on top of [Apache Spark](http://spark.apache.org/). It is loosely based on the paper from He, Yaobin, et al.
["MR-DBSCAN: a scalable MapReduce-based DBSCAN algorithm for heavily skewed data"](http://www.researchgate.net/profile/Yaobin_He/publication/260523383_MR-DBSCAN_a_scalable_MapReduce-based_DBSCAN_algorithm_for_heavily_skewed_data/links/0046353a1763ee2bdf000000.pdf). 

There is also a [visual guide](http://www.irvingc.com/visualizing-dbscan) that explains how the DBSCan algorithm works.

DBScan is used to cluster stops and detect most visited areas in which most of the locations been found

##### Graphx Graph Processing Engine

TODO: 

### License

DBSCAN on Spark is available under the Apache 2.0 license. 
See the [LICENSE](LICENSE) file for details.


### Credits

Stop Detection algorithm maintained by Piotr Mrowczynski, Gabriel Vilen and Ananya Chowdhury 
DBSCAN on Spark is maintained by Irving Cordova (irving@irvingc.com).





