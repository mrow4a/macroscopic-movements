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

object MovementsJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()

    // TODO: Without below lines, if spark cluster consists only of master node, it will not run and hang:
    //
    //  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //  conf.setMaster("local[*]").set("spark.executor.memory", "1g")
    //
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    if (args.length < 2) {
      throw new Exception("No s3 endpoint or s3a:// path given. "
        + "Example: http://localhost:9000 s3a://movements:movements@movements/macroscopic-movement-01_areafilter.csv")
    }
    var endpoint = args(0) // need to pass endpoint as arg
    var src = args(1) // need to pass file as arg

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)

    import spark.implicits._

    val data = spark.sparkContext.textFile(src)

    /*
     STOP DETECTION

     TODO: Mind that this will filter out all points outside Berlin!!!
     */
    val parsedData = data
      .map(s => s.split(';').toVector)
      // Filter point which cannot be processed by this job
      .filter(filterPoint)

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

    /*
     CLUSTERING

     TODO: Mind that this version of DBScan of spark is only compatible with Berlin!!!
          CHECK FILE util.Config and make it work across more areas
     */

    // Transform stops to include information about area they are in and eps of that area
    val areaBoundStops = detectedStops
      .map(assignArea)
      .cache()

    // Clusters stops in Berlin
    val clusteredPoints = DBSCAN.train(
      areaBoundStops,
      Config.eps,
      Config.maxPointsPerPartition
    ).labeledPoints.filter(_.cluster != 0)
      .cache()

    /*
     GRAPH ANALYSIS
     */
    val graphInput = clusteredPoints
      .map(point => (point.id, point.cluster))

    val createGraphJob = new CreateGraph()
    val createGraphJobDataframes = createGraphJob.graphOperations(graphInput, spark)

    val basicDF = clusteredPoints
      .map(point =>
        (point.cluster, point.x, point.y, point.duration) // TODO: cluster size not passed
      )
      .toDF("ClusterID", "Latitude", "Longitude", "Duration")
      .groupBy("ClusterID").avg("Latitude","Longitude","Duration")

    val countDF = clusteredPoints
      .map(point =>
        (point.cluster) // TODO: cluster size not passed
      )
      .toDF("ClusterID")
      .groupBy("ClusterID").count()

    val resultDf = basicDF
      .join(countDF, Seq("ClusterID"))
      .join(createGraphJobDataframes, Seq("ClusterID"))

    resultDf.collect()
      .map(row => row.mkString("|")).foreach(row => println(row))

    spark.stop()
  }

  private def filterPoint(point: Vector[String]): Boolean = {
    try{
      val outsideArea: DBSCANRectangle =
        DBSCANRectangle(Config.outsideBerlin.xMin,
          Config.outsideBerlin.xMax,
          Config.outsideBerlin.yMin,
          Config.outsideBerlin.yMax)
      val parsedPoint = Point(point)
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

  // TODO: Mind that this will assign areas only in Berlin!!!
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


    val parsedPoint = Point(point)
    var newPoint = point.toBuffer
    // Try to obtain most essential values
    if (innerArea.contains(parsedPoint.lat, parsedPoint.long)) {
      newPoint += Config.innerBerlin.id.toString
      newPoint += Config.innerBerlin.eps.toString
      newPoint += Config.innerBerlin.minPts.toString
    }
    else if (middleArea.contains(parsedPoint.lat, parsedPoint.long)) {
      newPoint += Config.middleBerlin.id.toString
      newPoint += Config.middleBerlin.eps.toString
      newPoint += Config.middleBerlin.minPts.toString
    }
    else {
      newPoint += Config.outsideBerlin.id.toString
      newPoint += Config.outsideBerlin.eps.toString
      newPoint += Config.outsideBerlin.minPts.toString
    }

    newPoint.toVector
  }
}