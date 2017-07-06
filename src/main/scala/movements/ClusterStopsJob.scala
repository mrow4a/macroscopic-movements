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
import util.Config

object ClusterStopsJob {

  val log: Logger = LoggerFactory.getLogger(ClusterStopsJob.getClass)

  def main(args: Array[String]) {

    var maxPointsPerPartition: Int = Config.maxPointsPerPartition
    var eps: Double = Config.eps
    var minPoints: Int = Config.minPoints

    if (args.length < 1) {
      log.error("Error: No input file given")
      System.exit(1)
    }
    var src = args(0) // need to pass file as arg

    log.info("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"DBSCAN")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    log.info("Parse Input File to StopPoint class instances")
    val data = sc.textFile(src)

    val parsedData = data.map(s => s.split(';').toVector)

    log.info("Filter Moves to obtain stops only")

    val detectedStops = StopDetection.filter(
      parsedData,
      Config.durationsSlidingWindowSize,
      Config.mobilityIndexThreshold,
      Config.stopAccuracyDistance,
      Config.stopAccuracySpeed,
      Config.minimumFlightSpeed,
      Config.minimumFlightDistance,
      Config.minimumAccuracyDistance,
      Config.minimumAccuracyDuration
    )

    // hardcoded areas for inner and outer berlin
    val innerArea: DBSCANRectangle = DBSCANRectangle(Config.innerBerlin.xMin,
      Config.innerBerlin.xMax,
      Config.innerBerlin.yMin,
      Config.innerBerlin.yMax)

    val middleArea: DBSCANRectangle =
      DBSCANRectangle(Config.middleBerlin.xMin,
        Config.middleBerlin.xMax,
        Config.middleBerlin.yMin,
        Config.middleBerlin.yMax)

    val innerStops = detectedStops.filter(p => innerArea.contains(DBSCANPoint(p)))
    // inner
    val outerStops = detectedStops.filter(p => !middleArea.contains(DBSCANPoint(p)))
    // outer
    val middleStops = detectedStops.subtract(outerStops).subtract(innerStops) // middle = detected - outer - inner

    log.debug("Cluster Points")

    // run DBSCAN for 3 areas with hardcoded parameters and merge
    val innerDBSCAN = runDBSCAN(innerStops, 0.001, minPoints, maxPointsPerPartition, 1)
    val middleDBSCAN = runDBSCAN(middleStops, 0.003, minPoints, maxPointsPerPartition, 2)
    val outerDBSCAN =  runDBSCAN(outerStops, 0.005, minPoints, maxPointsPerPartition, 3)

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


  private def writeToFile(clusteredData: RDD[String], eps: Double, minPoints: Int) = {
    log.debug("Save points to the result file")

    val random = new Random()
    var filePath = "resources/Locker/dbscan/" + eps + "_" + minPoints + "_" + random.nextInt()
    clusteredData.coalesce(1).saveAsTextFile(filePath)
    println("Wrote result to " + filePath)
  }
}