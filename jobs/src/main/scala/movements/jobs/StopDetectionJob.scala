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

    if (args.length < 1) {
      throw new Exception("No input file e.g. macroscopic-movement-01_areafilter.csv")
    }
    var src = args(0) // need to pass file as arg

    val data = spark.sparkContext.textFile(src)
    val dataCount = data.count()
    val parsedData = data
      .map(s => s.split(';').toVector)
      // Filter point which cannot be processed by this job
      .filter(filterPoint)

    val countUsers = parsedData
      .groupBy(p => p(2)).count()
    val parsedDataCount = parsedData.count()

    val detectedStops = StopDetection.run(
      parsedData,
      Config.durationsSlidingWindowSize,
      Config.mobilityIndexThreshold,
      Config.distanceThreshold,
      Config.speedThreshold,
      Config.minimumFlightSpeed,
      Config.minimumFlightDistance,
      Config.minimumAccuracyDistance,
      Config.minimumAccuracyDuration
    )

//    detectedStops.collect()
//      .foreach(row => println(row(0)+","+row(1)+","+row(2)+","+row(3)+","+row(4)+","+row(5)))

    val pointsPerUser = parsedData
      .groupBy(p => p(2)).values
      .map(points => points.toList.size)
    val stopsPerUser = detectedStops
      .groupBy(p => p(2)).values
      .map(stops => stops.toList.size)
    val stopsPerUserCount = stopsPerUser.count
    val num_bins = 20
    val (startValues,counts) = pointsPerUser.histogram(num_bins)
    val countsStops = stopsPerUser.histogram(startValues)
    spark.stop()
    println("Bins points")
    startValues.foreach(value => println(value))
    println("Bins points count")
    counts.foreach(value => println(value))
    println("Bin stops count")
    countsStops.foreach(value => println(value))

    println("There is total " + dataCount + " points")
    println("After filtering found " + parsedDataCount + " points and "+countUsers+" users")
    println("Number of users having at least one stop: "+stopsPerUserCount)
  }

  private def filterPoint(point: Vector[String]): Boolean = {
    try{
      val (xMin, xMax, yMin, yMax) = (52.0102, 11.2830, 53.0214, 13.9389)

      val outsideArea: DBSCANRectangle =
        DBSCANRectangle(xMin, xMax, yMin, yMax)

      val parsedPoint = Point(point)
      if (outsideArea.contains(parsedPoint.lat, parsedPoint.long)){
        true
      } else {
        false
      }
    } catch {
      case e: Exception => {
        println("Filtering point: "+ point.toString())
        false
      }
    }
  }
}