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
package stopdetection

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Top level method for calling StopDetection
  */
object StopDetection {

  /**
    * Filter detected points to find stops
    *
    * @param data parsed points stored as `RDD[[Vector[String]]`
    * Vector[String] should contain following data
    * at specific positions (0)->Latitude (1)->Longitude, (2)->ID, (3)->TimeStamp
    *
    * @param durationsWindowSize
    * @param mobilityIndexThreshold
    * @param minimumFlightSpeed
    * @param minimumFlightDistance
    * @param minimumAccuracyDistance
    * @param minimumAccuracyDuration
    */
  def filter(
      data: RDD[Vector[String]],
      durationsWindowSize: Double,
      mobilityIndexThreshold: Double,
      minimumFlightSpeed: Double,
      minimumFlightDistance: Double,
      minimumAccuracyDistance: Double,
      minimumAccuracyDuration: Double): RDD[Vector[String]] = {

    new StopDetection(
      durationsWindowSize,
      mobilityIndexThreshold,
      minimumFlightSpeed,
      minimumFlightDistance,
      minimumAccuracyDistance,
      minimumAccuracyDuration).filter(data)
  }
}

class StopDetection private(
      val durationsWindowSize: Double,
      val mobilityIndexThreshold: Double,
      val minimumFlightSpeed: Double,
      val minimumFlightDistance: Double,
      val minimumAccuracyDistance: Double,
      val minimumAccuracyDuration: Double)
  extends Serializable {

  /* Constants */
  def minDuration: Double = 0.0001 // default to very small number in seconds
  def maxDuration: Double = 86400 // default to 24h in seconds

  /**
    * This function filters all DetectedPoints and
    * return Vector with (0)->Latitude (1)->Longitude, (2)->ID, (3)->TimeStamp,
    */
  private def filter(parsedData: RDD[Vector[String]]): RDD[Vector[String]] = {
    parsedData
      // Convert to Detected Point
      .map(DetectedPoint)
      // Split into several partitions by ID
      .groupBy(_.id).values
      // Filter movements for each group in parallel (map) and
      // then flatten (merge into RDD)
      .flatMap(getCandidates)
  }

  private def getCandidates(userValues: Iterable[DetectedPoint]): Iterator[Vector[String]] = {
    val sortedValues = userValues
      // Ensure that values are sorted by timestamp
      .toList.sortBy(_.timestamp)

    // First point is always a stop
    val firstPoint = new StopCandidatePoint(sortedValues.head.vector)
    firstPoint.isStop = true

    sortedValues
      // Visit each detected point and create list of StopCandidatePoint
      .sliding(2).filter(_.size==2).map(determineMetadata)
      // Filter Anomalies
      .filter(filterAnomalies)
      .map(u => u.vector)
  }

  private def determineMetadata(window: List[DetectedPoint])
  : StopCandidatePoint = {
    val lastPoint = window(0)
    val current = window(1)
    // Determine duration, distance and speed between previous and current point
    val timeDifference = determineTimeDifference(
      lastPoint.timestamp, current.timestamp)
    val distance = determineDistance(
      lastPoint.lat,
      lastPoint.long,
      current.lat,
      current.long)
    val speed = determineSpeed(distance, timeDifference)

    // Add to feedback loop lists
    var resultPoint = new StopCandidatePoint(current)
    resultPoint.distance = distance
    resultPoint.duration = timeDifference
    resultPoint
  }

  private def filterAnomalies(point: StopCandidatePoint)
  : Boolean = {
    val distance = point.distance
    val duration = point.duration
    val speed = determineSpeed(distance, duration)

    if ((speed > minimumFlightSpeed && distance < minimumFlightDistance) ||
      (distance < minimumAccuracyDistance && duration < minimumAccuracyDuration)) {
      false
    } else {
      true
    }
  }

  private def determineMobilityIndex(durationsList: ArrayBuffer[Double])
  : Double = {
    // Sum of the inversions
    durationsList.map(duration => 1/duration).sum
  }

  private def determineSpeed(distance: Double, duration: Double): Double = {
    distance / duration
  }

  private def determineDistance(lat1: Double, lng1: Double,
                                lat2: Double, lng2: Double): Double = {
    val earthRadius = 6371000
    val dLat = Math.toRadians(lat2 - lat1)
    val dLng = Math.toRadians(lng2 - lng1)

    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLng / 2) * Math.sin(dLng / 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (earthRadius * c).toFloat
  }

  private def determineTimeDifference(lastTimestamp: Int, currentTimestmap: Int): Double = {
    var pointsDuration = (currentTimestmap - lastTimestamp).toDouble
    if (pointsDuration == 0) {
      pointsDuration = minDuration
    }

    pointsDuration
  }
}

