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
    * @param durationsSlidingWindowSize
    * @param stopCertaintyMaxDistance
    * @param stopCertaintyMaxSpeed
    * @param travelCertaintyMinSpeed
    * @param mobilityIndexThreshold
    * @param filterSpeedThreshold
    * @param filterDistanceThreshold
    * @param filterDurationThreshold
    */
  def filter(
      data: RDD[Vector[String]],
      durationsSlidingWindowSize: Double,
      stopCertaintyMaxDistance: Double,
      stopCertaintyMaxSpeed: Double,
      travelCertaintyMinSpeed: Double,
      mobilityIndexThreshold: Double,
      filterSpeedThreshold: Double,
      filterDistanceThreshold: Double,
      filterDurationThreshold: Double): RDD[Vector[String]] = {

    new StopDetection(
      durationsSlidingWindowSize,
      stopCertaintyMaxDistance,
      stopCertaintyMaxSpeed,
      travelCertaintyMinSpeed,
      mobilityIndexThreshold,
      filterSpeedThreshold,
      filterDistanceThreshold,
      filterDurationThreshold).filter(data)
  }
}

class StopDetection private(
      val durationsSlidingWindowSize: Double,
      val stopCertaintyMaxDistance: Double,
      val stopCertaintyMaxSpeed: Double,
      val travelCertaintyMinSpeed: Double,
      val mobilityIndexThreshold: Double,
      val filterSpeedThreshold: Double,
      val filterDistanceThreshold: Double,
      val filterDurationThreshold: Double)
  extends Serializable {

  /* Constants */
  def minDuration: Double = 0.0001 // default to very small number in seconds
  def maxDuration: Double = 86400 // default to 24h in seconds

  /**
    * This function filters all DetectedPoints and
    * return Vector with (0)->Latitude (1)->Longitude, (2)->ID, (3)->TimeStamp,
    */
  private def filter(parsedData: RDD[Vector[String]]): RDD[StopCandidatePoint] = {
    parsedData
      // Convert to Detected Point
      .map(DetectedPoint)
      // Split into several partitions by ID
      .groupBy(_.id).values
      // Filter movements for each group in parallel (map) and
      // then flatten (merge into RDD)
      .flatMap(getCandidates)
  }

  private def getCandidates(userValues: Iterable[DetectedPoint]): Iterable[StopCandidatePoint] = {
    val initPoint = new StopCandidatePoint(Vector())
    userValues
      // Ensure that values are sorted by timestamp
      .toList.sortBy(_.timestamp)
      // Visit each detected point and create list of StopCandidatePoint
      .sliding(2).map(determineMetadata)
      // Visit each pair of detected points and create list of StopCandidatePoints
      .scanLeft((initPoint, ArrayBuffer[Double]()))(determineCurrentMobilityIndex)
      .map(_._1)
      .scanRight(initPoint)(determineNextMobilityIndex)
  }

  private def determineMetadata(window: List[DetectedPoint])
  : StopCandidatePoint = {
    if (window.size == 1) {
      // If there is only one point for this user, there is no metadata
      val current = window(0)
      var resultPoint = new StopCandidatePoint(current)
      resultPoint.speed = filterSpeedThreshold
      resultPoint.distance = filterDistanceThreshold
      resultPoint.duration = filterDurationThreshold
      resultPoint
    } else {
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
      resultPoint.speed = speed
      resultPoint.distance = distance
      resultPoint.duration = timeDifference
      resultPoint
    }
  }

  private def determineCurrentMobilityIndex(result: (StopCandidatePoint, ArrayBuffer[Double]),
                      current: StopCandidatePoint)
  : (StopCandidatePoint, ArrayBuffer[Double]) = {
    // Obtain parameters
    var lastPoint = result._1
    var durationsSlidingWindow = result._2

    if (durationsSlidingWindow.isEmpty) {
      durationsSlidingWindow += maxDuration
      (current, durationsSlidingWindow)
    } else {
      val timeDifference = determineTimeDifference(
        lastPoint.timestamp, current.timestamp)
      val distance = determineDistance(
        lastPoint.lat,
        lastPoint.long,
        current.lat,
        current.long)
      val speed = determineSpeed(distance, timeDifference)

      var isAnomaly = false
      // Add basic metadata
      var resultPoint = new StopCandidatePoint(current)

      // Dont take into account points which are anomalies
      resultPoint.speed = speed
      resultPoint.distance = distance
      resultPoint.duration = timeDifference

      // Determine duration sliding window and mobility index
      durationsSlidingWindow += timeDifference

      val updatedSlidingWindow = determineSlidingWindow(durationsSlidingWindow)
      resultPoint.previousMobilityIndex = lastPoint.mobilityIndex
      resultPoint.mobilityIndex = determineMobilityIndex(updatedSlidingWindow)

      (resultPoint, updatedSlidingWindow, isAnlomaly)

    }
  }

  private def determineNextMobilityIndex(currentPoint: StopCandidatePoint,
                      next: StopCandidatePoint)
  : StopCandidatePoint = {
    // Obtain parameters
    currentPoint.nextMobilityIndex = next.mobilityIndex
    currentPoint
  }

  private def determineStop(currentResultPoint: StopCandidatePoint)
  : (DetectedPoint, Boolean) = {
    if(isPossibleStop(currentResultPoint.speed, currentResultPoint.distance)) {
      (currentResultPoint, true)
    } else {
      (currentResultPoint, false)
    }
  }

  private def determineMobilityIndex(durationsList: ArrayBuffer[Double])
  : Double = {
    // Sum of the inversions
    durationsList.map(duration => 1/duration).sum
  }

  private def determineSlidingWindow(durationsList: ArrayBuffer[Double])
  : ArrayBuffer[Double] = {
    // Go from last to first and get trimmed duration list and mobility index
    val result = durationsList.foldRight((ArrayBuffer[Double](), 0.0)) { (current, result) => {
        // Obtain parameters
        var newDurationsList = result._1
        var totalDuration = result._2

        totalDuration += current // increase sum of past durations
        // Check if sum of duration did not exceed the maximum sliding window duration
        if (totalDuration < durationsSlidingWindowSize || newDurationsList.isEmpty) {
          newDurationsList += current // add current duration to the result sliding window
        }

        (newDurationsList, totalDuration)
      }
    }
    result._1
  }

  private def isPossibleStop(distance: Double, speed: Double): Boolean = {
    if (speed < stopCertaintyMaxSpeed && distance < stopCertaintyMaxDistance
    || speed < travelCertaintyMinSpeed && distance > stopCertaintyMaxDistance) {
      true
    } else {
      false
    }
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

