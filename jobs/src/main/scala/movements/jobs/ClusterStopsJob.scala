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

import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.{SparkConf, SparkContext}
import stopdetection.StopDetection

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

    val parsedData = data.map(s => s.split(';').toVector)

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
    ).collect()

    val detectedStopsParal = sc.parallelize(detectedStops)

    val dbScanModel = DBSCAN.train(
      detectedStopsParal,
      Parameters.eps,
      Parameters.minPoints,
      Parameters.maxPointsPerPartition)

    val clusteredData = dbScanModel.labeledPoints
      .filter(_.cluster != 0)
      .map(p => (p.x, p.y, p.cluster, p.duration))
      .groupBy(a => a._3).values
      .map(p => getMetadata(p))

    clusteredData.collect().foreach(stop => println(stop))

    sc.stop()
  }

  private def getMetadata(data: Iterable[(Double, Double, Int, Double)]): String = {
    var countVal = 0.0
    var lat = 0.0
    var lon = 0.0
    var duration = 0.0

    val avg = data.foldLeft(ArrayBuffer[(Double, Double, Int, Double)]()) { (result, c) => {
      countVal += 1
      lat += c._1
      lon += c._2
      duration += c._4

      var tmp = (lat / countVal, lon / countVal, c._3, duration)
      result += tmp
    }
    }.last // end of foldLeft

    val avgLat = avg._1
    val avgLon = avg._2
    val cluster = avg._3
    val avgDuration = avg._4 / countVal

    avgLat.toString + "," + avgLon.toString + "," + cluster.toString + "," + avgDuration.toInt.toString
  }
}