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
    if (args.length < 2) {
      println("Error: No input or output file given")
      println("Example: " +
        "/home/mrow4a/Projects/MacroMovements/resources/Locker/input.csv " +
        "/home/mrow4a/Projects/MacroMovements/resources/Locker/dbscan")
      System.exit(1)
    }
    var src = args(0) // need to pass file as arg
    var dst = args(1) // need to pass file as arg

    println("Create Spark Context")
    val conf = new SparkConf().setAppName(s"MOVEMENTS")

    // NOTE: Without below lines, if spark cluster consists only of master node,
    // it will not run:
    //
    //  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //  conf.setMaster("local[*]").set("spark.executor.memory", "1g")
    //
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
    ).collect()

    val detectedStopsParal = sc.parallelize(detectedStops)

    val dbScanModel = DBSCAN.train(
      detectedStopsParal,
      Parameters.eps,
      Parameters.minPoints,
      Parameters.maxPointsPerPartition)

    val clusteredData = dbScanModel.labeledPoints
      .filter(_.cluster != 0)
      .map(p => (p.x, p.y, p.cluster))
      .groupBy(a=> (a._3)).values
      .map(p => getMetadata(p))


    clusteredData.saveAsTextFile(dst)
    println("Stopping Spark Context...")
    sc.stop()
  }

  private def getMetadata(data : Iterable[(Double,Double,Int)]) :
  String = {
    var countVal = 0
    var Lat = 0.0
    var Long = 0.0
    val resultAvg = data.foldLeft(ArrayBuffer[(Double,Double,Int)]()) { (result, c) => {
      countVal += 1
      Lat += c._1
      Long += c._2
      val temp = ((Lat/countVal) , (Long/countVal) , c._3)
      result += temp
    }
    }.last      // end of foldLeft
    val lat = resultAvg._1
    val long = resultAvg._2
    val cluster = resultAvg._3
    val resultFinal = lat.toString + "," + long.toString + "," + cluster.toString;
    resultFinal
  }


}