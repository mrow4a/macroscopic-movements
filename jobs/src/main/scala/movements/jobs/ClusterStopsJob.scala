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

import org.apache.spark.mllib.clustering.dbscan.{DBSCAN, DBSCANLabeledPoint, DBSCANRectangle}
import org.apache.spark.{SparkConf, SparkContext}
import stopdetection.{DetectedPoint, StopDetection}
import util.Config

import scala.collection.mutable.ArrayBuffer;

object ClusterStopsJob {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(s"MOVEMENTS")

    // NOTE: Without below lines, if spark cluster consists only of master node,
    // it will not run:
    //
    //  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //  conf.setMaster("local[*]").set("spark.executor.memory", "1g")
    //
    val sc = new SparkContext(conf)

    if (args.length < 2) {
      throw new Exception("No s3 endpoint or s3a:// path given. "
        + "Example: http://localhost:9000 s3a://movements:movements@movements/macroscopic-movement-01_areafilter.csv")
    }
    var endpoint = args(0) // need to pass endpoint as arg
    var src = args(1) // need to pass file as arg
    var dst = args(2) // need to pass dst as arg

    sc.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)

    val data = sc.textFile(src)

    val parsedData = data
      .map(s => s.split(';').toVector)
      // Filter point which cannot be processed by this job
      .filter(filterPoint)

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

    // Transform stops to include information about area they are in and eps of that area
    val areaBoundStops = detectedStops
      .map(assignArea).cache()

    // NOTE: Mind that this version of DBScan of spark is only compatible with Berlin!!!
    val dbscanModel = DBSCAN.train(
      areaBoundStops,
      Config.eps,
      Config.minPoints,
      Config.maxPointsPerPartition
    )

    val clusteredData = dbscanModel.labeledPoints
      .filter(_.cluster != 0)
      .groupBy(p => p.cluster).values
      .map(p => getMetadata(p))

    clusteredData.collect().foreach(stop => println(stop))

    sc.stop()
  }


  private def getMetadata(points : Iterable[DBSCANLabeledPoint]) :
  String = {
    var countVal : Int = 0
    var lat = 0.0
    var lon = 0.0
    var duration = 0.0

    val avg = points.foldLeft(ArrayBuffer[(Double, Double, Int, Double)]()) {
      (result, c) => {
        countVal += 1
        lat += c.x
        lon += c.y
        duration += c.duration

        var tmp = (lat / countVal, lon / countVal, c.cluster, duration)
        result += tmp
      }
    }.last // end of foldLeft


    val avgLat  = avg._1
    val avgLon  = avg._2
    val cluster = avg._3
    val avgDuration = BigDecimal((avg._4 / countVal)/3600).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    avgLat.toString + "," + avgLon.toString + "," + cluster.toString + "," + avgDuration.toString + "," + countVal.toString
  }

  private def filterPoint(point: Vector[String]): Boolean = {
    try{
      val outsideArea: DBSCANRectangle =
        DBSCANRectangle(Config.outsideBerlin.xMin,
          Config.outsideBerlin.xMax,
          Config.outsideBerlin.yMin,
          Config.outsideBerlin.yMax)
      val parsedPoint = DetectedPoint(point)
      // Try to obtain most essential values
      parsedPoint.id
      parsedPoint.lat
      parsedPoint.long
      parsedPoint.timestamp
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

  private def assignArea(point: Vector[String]): Vector[String] = {
    val innerArea: DBSCANRectangle = DBSCANRectangle(Config.innerBerlin.xMin,
      Config.innerBerlin.xMax,
      Config.innerBerlin.yMin,
      Config.innerBerlin.yMax)

    val middleArea: DBSCANRectangle =
      DBSCANRectangle(Config.middleBerlin.xMin,
        Config.middleBerlin.xMax,
        Config.middleBerlin.yMin,
        Config.middleBerlin.yMax)


    val parsedPoint = DetectedPoint(point)
    var newPoint = point.toBuffer
    // Try to obtain most essential values
    if (innerArea.contains(parsedPoint.lat, parsedPoint.long)) {
      newPoint += Config.innerBerlin.id.toString
      newPoint += Config.innerBerlin.eps.toString
    }
    else if (middleArea.contains(parsedPoint.lat, parsedPoint.long)) {
      newPoint += Config.middleBerlin.id.toString
      newPoint += Config.middleBerlin.eps.toString
    }
    else {
      newPoint += Config.outsideBerlin.id.toString
      newPoint += Config.outsideBerlin.eps.toString
    }

    newPoint.toVector
  }
}