/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package util

import movements.ClusterStopsJob
import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import stopdetection.StopDetection

/**
  * Created by gabri on 2017-06-01.
  */
object DBSCANrunner {

  val log = LoggerFactory.getLogger(ClusterStopsJob.getClass)

  def main(args: Array[String]) {
    log.info("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"DBSCAN histogram")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    log.info("Parse Input File to StopPoint class instances")
    val src = "resources\\Locker\\macroscopic-movement-01_areafilter.csv"
    val data = sc.textFile(src)

    val parsedData = data.map(s => s.split(';').toVector)

    log.info("Filter Moves to obtain stops only")

    val durationsSlidingWindowSize = 1800.0 // By default 20 minutes
    val mobilityIndexThreshold = 0.0017 // Mobility Index Threshold used to determine mobility patterns
    val stopAccuracyDistance = 1000 // meters
    val stopAccuracySpeed = 1.4 // m/s

    // Parameters for anomaly filtering
    val minimumFlightSpeed = 83 // Filter all speeds above 300 km/h
    val minimumFlightDistance = 100000 // Filter all speeds above 300 km/h with distances over 100km
    val minimumAccuracyDistance = 100 // Filter all points within distance of 100m, anomalies
    val minimumAccuracyDuration = 100 // Filter all points within duration of 100s, anomalies

    val detectedStops = StopDetection.filter(
      parsedData,
      durationsSlidingWindowSize,
      mobilityIndexThreshold,
      stopAccuracyDistance,
      stopAccuracySpeed,
      minimumFlightSpeed,
      minimumFlightDistance,
      minimumAccuracyDistance,
      minimumAccuracyDuration
    )

   // val dstFile = new File("resources/Locker/dbscan/histogram")
  //  val bufferedWriter = new BufferedWriter(new FileWriter(dstFile))

    var maxPointsPerPartition = 10000
    var eps = 0.0029
    var minPoints = 5

    log.debug("Cluster Points")
    val runs = 11
    for (i <- 0 to runs) {
      val dbScanModel = DBSCAN.train(
        detectedStops,
        eps,
        minPoints,
        maxPointsPerPartition)

      eps += 0.0001
      // minPoints += 1

    }
    Writer.close()
  }

  /*
      def main(args: Array[String]) {

      val dstFile = new File("resources/Locker/dbscan/histogram")
      val bw = new BufferedWriter(new FileWriter(dstFile))

      // resources\Locker\macroscopic-movement-01_areafilter.csv 100000 0.001 5
      val srcFile = "resources\\Locker\\macroscopic-movement-01_areafilter.csv"
      val partitionSize = 100000
      var radius = 0.001
      //radius = 0.001
      // 0.01 = 1.1 km, min distance between 2 points = 0.1112 km
      var minPts = 5
      val limit = 10
      for (i <- 0 to limit) {

        val array = Array(srcFile, partitionSize.toString, radius.toString, minPts.toString)

        minPts += 1
        val c = ClusterStopsJob.main(array)
        bw.write(text)

      }
      bw.close()
    }
    */


}
