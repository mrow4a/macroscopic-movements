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

package movements

import java.util.Random

import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import stopdetection.StopDetection

object ClusterStopsJob {

  val log = LoggerFactory.getLogger(ClusterStopsJob.getClass)

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("You must pass the arguments: " +
        "<src file> <max points per partition> <eps> <min points per partition>")
      System.exit(1)
    }

    log.info("Parse arguments of the function")
    val (src, maxPointsPerPartition, eps, minPoints) =
      (args(0), args(1).toInt, args(2).toFloat, args(3).toInt)

    log.info("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"DBSCAN(eps=$eps, min=$minPoints, max=$maxPointsPerPartition)")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    log.info("Parse Input File to StopPoint class instances")
    val data = sc.textFile(src)

    val parsedData = data.map(s => s.split(';').toVector)

    log.info("Filter Moves to obtain stops only")

    val durationsSlidingWindowSize = 2400.0 // By default 40 minutes
    val stopCertaintyMaxDistance = 1500.0 // By default max walking distance for human
    val stopCertaintyMaxSpeed = 0.833 // By default min human walking speed
    val travelCertaintyMinSpeed = 1.4 // By default max human walking speed
    val filterSpeedThreshold = 70 // Filter all speeds above 250 km/h
    val filterDistanceThreshold = 100 // Filter all points within distance of 100m, anomalies
    val filterDurationThreshold = 5 // Filter all points within duration of 5s, anomalies

    val detectedStops = StopDetection.filter(
      parsedData,
      durationsSlidingWindowSize,
      stopCertaintyMaxDistance,
      stopCertaintyMaxSpeed,
      travelCertaintyMinSpeed,
      filterSpeedThreshold,
      filterDistanceThreshold,
      filterDurationThreshold
    )

    log.debug("Cluster Points")

    val dbScanModel = DBSCAN.train(
       detectedStops,
       eps,
       minPoints,
       maxPointsPerPartition)

    val clusteredData = dbScanModel.labeledPoints.map(p => s"${p.id},${p.x},${p.y},${p.cluster}")

    log.debug("Save points to the result file")

    val random = new Random()
    var filePath = "resources/Locker/dbscan/" + eps + "_" + minPoints + "_" + random.nextInt()
    clusteredData.coalesce(1).saveAsTextFile(filePath)

    // clusteredData.foreach(clusteredPoint => println(clusteredPoint.toString()))

    log.info("Stopping Spark Context...")
    sc.stop()
  }
}