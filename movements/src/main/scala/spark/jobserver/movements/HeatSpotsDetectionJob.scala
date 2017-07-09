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
package spark.jobserver.movements

import com.typesafe.config.Config
import movements.Parameters
import org.apache.spark._
import spark.jobserver.api.{SparkJob => NewSparkJob, _}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}
import spark.jobserver.stopdetection.StopDetection
import org.apache.spark.mllib.clustering.dbscan.{DBSCAN, DBSCANPoint, DBSCANRectangle}
import org.apache.spark.rdd.RDD
import org.scalactic._

import scala.util.Try

object HeatSpotsDetectionJob extends NewSparkJob {
  type JobData = String
  type JobOutput = collection.Seq[String]

  def runJob(sc: SparkContext, runtime: JobEnvironment, sourcePath: JobData): JobOutput = {
//    val data = sc.textFile(sourcePath)
//    val parsedData = data.map(s => s.split(';').toVector)
//
//    val detectedStops = StopDetection.filter(
//          parsedData,
//          Parameters.durationsSlidingWindowSize,
//          Parameters.mobilityIndexThreshold,
//          Parameters.stopAccuracyDistance,
//          Parameters.stopAccuracySpeed,
//          Parameters.minimumFlightSpeed,
//          Parameters.minimumFlightDistance,
//          Parameters.minimumAccuracyDistance,
//          Parameters.minimumAccuracyDuration
//        ).collect()
//
//    val detectedStopsCol = sc.parallelize(detectedStops)

//    val dbScanModel = DBSCAN.train(
//      detectedStopsCol,
//      Parameters.eps,
//      Parameters.minPoints,
//      Parameters.maxPointsPerPartition)

//    val clusteredData = dbScanModel.labeledPoints.collect() //.map(p => s"${p.id},${p.x},${p.y},${p.cluster}")
//    val seq = clusteredData.toSeq
//    val seq = dbScanModel.labeledPoints.count().toString

    val detectedStops = Vector(
      Vector("52.527", "13.382", "469c6ac5b74151370fe219d241c74c104b3236cfcf0aafc11ba59e6154592bc2", "42883", "620.0"),
      Vector("52.549", "13.429", "469c6ac5b74151370fe219d241c74c104b3236cfcf0aafc11ba59e6154592bc2", "44147", "55534.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.588", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.998", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.589", "12.999", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "15501", "751.0"),
      Vector("52.599", "12.997", "207d841b59bbce78f439142a1d08c8e352a95270fed890313bf8c834bf79dc9f", "16318", "6808.0")
    )
    val detectedStopsCol = sc.parallelize(detectedStops)

    val dbScanModel = DBSCAN.train(
      detectedStopsCol,
      Parameters.eps,
      Parameters.minPoints,
      Parameters.maxPointsPerPartition)
    detectedStopsCol.map(v => v.toString).collect().toSeq
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.path"))
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.path param"))))
  }
}

///**
//  * TODO
// */
//object HeatSpotsDetectionJob extends SparkJob {
//
//  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
//    Try(config.getString("input.path"))
//      .map(x => SparkJobValid)
//      .getOrElse(SparkJobInvalid("No inputpath config param"))
//  }
//
//  override def runJob(sc: SparkContext, config: Config): Any = {
//    val sourcePath = config.getString("input.path")
//    val data = sc.textFile(sourcePath)
//
//    val parsedData = data.map(s => s.split(';').toVector)
//
//    val result = StopDetection.filter(
//      parsedData,
//      Parameters.durationsSlidingWindowSize,
//      Parameters.mobilityIndexThreshold,
//      Parameters.stopAccuracyDistance,
//      Parameters.stopAccuracySpeed,
//      Parameters.minimumFlightSpeed,
//      Parameters.minimumFlightDistance,
//      Parameters.minimumAccuracyDistance,
//      Parameters.minimumAccuracyDuration
//    ).map(v => v.toString).collect()
//
//    // hardcoded areas for inner and outer berlin
//    val innerArea: DBSCANRectangle = DBSCANRectangle(Parameters.innerBerlin.xMin,
//      Parameters.innerBerlin.xMax,
//      Parameters.innerBerlin.yMin,
//      Parameters.innerBerlin.yMax)
//
//    val middleArea: DBSCANRectangle =
//      DBSCANRectangle(Parameters.middleBerlin.xMin,
//        Parameters.middleBerlin.xMax,
//        Parameters.middleBerlin.yMin,
//        Parameters.middleBerlin.yMax)
//
//    val innerStops = detectedStops.filter(p => innerArea.contains(DBSCANPoint(p)))
//    // inner
//    val outerStops = detectedStops.filter(p => !middleArea.contains(DBSCANPoint(p)))
//    // outer
//    val middleStops = detectedStops.subtract(outerStops).subtract(innerStops) // middle = detected - outer - inner
//
//
//    // run DBSCAN for 3 areas with hardcoded parameters and merge
//    val innerDBSCAN = runDBSCAN(innerStops, 0.001, Parameters.minPoints, Parameters.maxPointsPerPartition, 1)
//    val middleDBSCAN = runDBSCAN(middleStops, 0.003, Parameters.minPoints, Parameters.maxPointsPerPartition, 2)
//    val outerDBSCAN =  runDBSCAN(outerStops, 0.005, Parameters.minPoints, Parameters.maxPointsPerPartition, 3)
//
//    // merge results and write to file
//    val result = innerDBSCAN ++ middleDBSCAN ++ outerDBSCAN
//
//    result
//  }
//
//  private def runDBSCAN(innerStops: RDD[Vector[String]],
//                        eps: Double, minPoints: Int,
//                        maxPointsPerPartition: Int,
//                        areaID: Int)
//  : RDD[String] = {
//    DBSCAN.train(innerStops, eps, minPoints, maxPointsPerPartition)
//      .labeledPoints.map(p => s"${p.id},${p.x},${p.y},${
//      if (p.cluster == 0) 0 else areaID + "" + p.cluster
//    }")
//  }
//}

