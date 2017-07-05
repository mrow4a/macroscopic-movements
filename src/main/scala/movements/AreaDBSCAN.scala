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

import org.apache.spark.mllib.clustering.dbscan.{DBSCAN, DBSCANPoint, DBSCANRectangle}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import stopdetection.StopDetection

/**
  * Runs area version of DBSCAN. Input parameter is one file, rest is hardcoded.
  *
  * Created by gabri on 2017-06-01.
  *
  */
object AreaDBSCAN {

  val log: Logger = LoggerFactory.getLogger(ClusterStopsJob.getClass)

  def main(args: Array[String]) {
    log.info("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"DBSCAN")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    log.info("Parse Input File to StopPoint class instances")
    val src = args(0)
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

    log.debug("Cluster Points")

    // Hardcoded areas for inner and outer Berlin
    val innerArea: DBSCANRectangle = DBSCANRectangle(52.4425, 13.2582, 52.5647, 13.4818)
    val middleArea: DBSCANRectangle = DBSCANRectangle(52.3446, 13.0168, 52.6375, 13.6603)

    val innerStops = detectedStops.filter(p => innerArea.contains(DBSCANPoint(p)))    // inner
    val outerStops = detectedStops.filter(p => !middleArea.contains(DBSCANPoint(p)))    // outer
    val middleStops = detectedStops.subtract(outerStops).subtract(innerStops) // middle = detected - outer - inner

    log.debug("Cluster Points")

    // hardcoded parameters
    var maxPointsPerPartition = 10000
    var eps = 0.001
    var minPoints = 5

    // run areaDBSCAN and merge
    val innerDBSCAN = runDBSCAN(innerStops, 0.001, minPoints, maxPointsPerPartition, 1)
    val middleDBSCAN = runDBSCAN(middleStops, 0.003, minPoints, maxPointsPerPartition, 2)
    val outerDBSCAN =  runDBSCAN(middleStops, 0.005, minPoints, maxPointsPerPartition, 3)

    // merge results and write to file
    writeToFile(innerDBSCAN ++ middleDBSCAN ++ outerDBSCAN, eps, minPoints)

    log.info("Stopping Spark Context...")
    sc.stop()
  }

  private def runDBSCAN(innerStops: RDD[Vector[String]],
                        eps: Double, minPoints: Int,
                        maxPointsPerPartition: Int,
                        areaID: Int)
  : RDD[String] = {
    DBSCAN.train(innerStops, eps, minPoints, maxPointsPerPartition)
      .labeledPoints.map(p => s"${p.id},${p.x},${p.y},${
      if (p.cluster == 0) 0 else areaID + "" + p.cluster
    }")
  }

  val random = new Random()

  private def writeToFile(clusteredData: RDD[String], eps: Double, minPoints: Int) = {
    log.debug("Save points to the result file")

    val filePath = "resources/Locker/dbscan/v2/" + eps + "_" + minPoints + "_" + random.nextInt()
    clusteredData.coalesce(1).saveAsTextFile(filePath)

    log.info("Wrote result to " + filePath)
  }

}
