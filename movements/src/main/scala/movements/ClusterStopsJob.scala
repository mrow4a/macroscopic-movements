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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import spark.jobserver.stopdetection.StopDetection

object ClusterStopsJob {

  def main(args: Array[String]) {
    var src = "/data/macroscopic-movement-01_areafilter.csv"

    println("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"MOVEMENTS")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    println("Parse Input File to StopPoint class instances")
    val data = sc.textFile(src)

    val parsedData = data.map(s => s.split(';').toVector)

    println("Filter Moves to obtain stops only")

    val detectedStops = StopDetection.filter(
      parsedData,
      Parameters.durationsSlidingWindowSize,
      Parameters.mobilityIndexThreshold,
      Parameters.stopAccuracyDistance,
      Parameters.stopAccuracySpeed,
      Parameters.minimumFlightSpeed,
      Parameters.minimumFlightDistance,
      Parameters.minimumAccuracyDistance,
      Parameters.minimumAccuracyDuration
    )

    val dbScanModel = DBSCAN.train(
      detectedStops,
      Parameters.eps,
      Parameters.minPoints,
      Parameters.maxPointsPerPartition)

    val clusteredData = dbScanModel.labeledPoints.collect().map(p => s"${p.id},${p.x},${p.y},${p.cluster}")

    clusteredData.map(p => println(p))
    //detectedStops.map(p => println(p))
    println("Stopping Spark Context...")
    sc.stop()
  }

}