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

package movements.jobs

import graph.CreateGraph
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.dbscan.{DBSCAN, DBSCANRectangle}
import org.apache.spark.sql.SparkSession
import stopdetection.{Point, StopDetection}
import util.Config

object StopDetectionJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]").set("spark.executor.memory", "1g")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    if (args.length < 2) {
      throw new Exception("No input file e.g. macroscopic-movement-01_areafilter.csv")
    }
    var src = args(1) // need to pass file as arg

    val data = spark.sparkContext.textFile(src)

    val parsedData = data
      .map(s => s.split(';').toVector)

    val detectedStops = StopDetection.run(
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

    detectedStops.collect().foreach(row => println(row))

    spark.stop()
  }
}