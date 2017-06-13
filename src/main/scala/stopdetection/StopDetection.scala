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
      filterSpeedThreshold: Double,
      filterDistanceThreshold: Double,
      filterDurationThreshold: Double): RDD[Vector[String]] = {

    new StopDetection(
      durationsSlidingWindowSize,
      stopCertaintyMaxDistance,
      stopCertaintyMaxSpeed,
      travelCertaintyMinSpeed,
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
  private def filter(parsedData: RDD[Vector[String]]): RDD[Vector[String]] = {
    parsedData
      // Convert to Detected Point
      .map(DetectedPoint)
      // Split into several partitions by ID
      .groupBy(_.id).values
      // Filter movements for each group in parallel (map) and
      // then flatten (merge into single RDD)
      .flatMap(filterMovements)
  }

  private def filterMovements(userValues: Iterable[DetectedPoint]): Iterator[Vector[String]] = {
    val initStopCandidatePoint = new StopCandidatePoint(Vector("","","",""))
    // Convert to list and ensure that values are sorted be timestamp
    userValues
      .toList.sortBy(_.timestamp)
      // Visit each detected point and create list of StopCandidatePoint
      .sliding(2).map(determineMetadata)
      // TODO
      .scanLeft((initStopCandidatePoint, ArrayBuffer[Double]()))(analyze)
      // TODO
      .drop(1).map(_._1)
      // Take window of 3 StopCandidatePoint's and determine stops in each groups
      .sliding(2).map(determineStop)
      // Return only points which were detected as stops
      .filter(_._2 == true)
      // Return only points
      .map(_._1)
      // Return result Vector
      .map(resultPair => Vector(resultPair.lat.toString,
                                resultPair.long.toString,
                                resultPair.id.toString,
                                resultPair.timestamp.toString)
      )
  }

  private def analyze(result: (StopCandidatePoint, ArrayBuffer[Double]),
                      current: StopCandidatePoint)
  : (StopCandidatePoint, ArrayBuffer[Double]) = {
    // Obtain parameters
    var durationsSlidingWindow = result._2

    // Determine mobility indexes and new duration sliding window
    durationsSlidingWindow += current.duration
    val determinationResult = determineSlidingWindow(durationsSlidingWindow)
    val updatedSlidingWindow = determinationResult._1
    val mobilityIndex = determinationResult._2

    val behaviourType = determineBehaviour(current.distance, current.speed)
    current.mobilityIndex = mobilityIndex
    current.behaviourType = behaviourType
    (current, updatedSlidingWindow)

  }

  private def determineStop(window: Seq[StopCandidatePoint])
  : (DetectedPoint, Boolean) = {
    if (window.size == 1) {
      // If window is equal to two, there is no stop
      // since both stop candidates are the same points
      val currentResult = window(0)
      if (currentResult.behaviourType == BehaviourType.Stop
          || currentResult.behaviourType == BehaviourType.PossibleStop) {
        (currentResult, true)
      } else {
        (currentResult, false)
      }
    } else {
      val previousResult = window(0)
      val currentResult = window(1)

      if (currentResult.behaviourType == BehaviourType.Stop) {
        (currentResult, true)
      } else if (currentResult.behaviourType == BehaviourType.PossibleStop
        && previousResult.mobilityIndex > currentResult.mobilityIndex) {
        (previousResult, true)
      } else {
        (currentResult, false)
      }
    }
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

  private def determineSlidingWindow(durationsList: ArrayBuffer[Double])
  : (ArrayBuffer[Double], Double, Double) = {
    // Go from last to first and get trimmed duration list and mobility index
    durationsList.foldRight((ArrayBuffer[Double](), 0.0, 0.0)) { (current, result) => {
      // Obtain parameters
      var newDurationsList = result._1
      var mobilityIndex = result._2
      var totalDuration = result._3

      totalDuration += current // increase sum of past durations
      // Check if sum of duration did not exceed the maximum sliding window duration
      if (totalDuration < durationsSlidingWindowSize || newDurationsList.isEmpty) {
        mobilityIndex += 1.0 / current // recalculate mobility index
        newDurationsList += current // add current duration to the result sliding window
      }

      (newDurationsList, mobilityIndex, totalDuration)
    }
    }
  }

  private def determineBehaviour(distance: Double, speed: Double): BehaviourType.Type = {
    var result = BehaviourType.Travel
    if (speed < stopCertaintyMaxSpeed && distance < stopCertaintyMaxDistance) {
      result = BehaviourType.Stop
    }
    else if (speed > stopCertaintyMaxSpeed && distance < stopCertaintyMaxDistance) {
      result = BehaviourType.PossibleTravel
    }
    else if (speed < travelCertaintyMinSpeed) {
      result = BehaviourType.PossibleStop
    }

    result
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

