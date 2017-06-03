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
    * TODO: it might need some configuration parameters
    */
  def filter(data: RDD[Vector[String]]): RDD[Vector[String]] = {
    new StopDetection().filter(data)
  }
}

class StopDetection private() extends Serializable {

  /**
    * Constants
    */

  // default to very small number in seconds
  def minDuration = 0.0001

  // default to 24h in seconds
  def maxDuration = 86400

  // 30 minutes in seconds
  def slidingWindowThreshold = 1800

  // meters
  def maxWalkDistanceHuman = 1000.0

  // meters per second
  def minWalkSpeedHuman = 0.833

  // meters per second
  def maxWalkSpeedHuman = 1.4

  // meters per second
  def maxTransportSpeed = 50

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
    // Convert to list and ensure that values are sorted be timestamp
    val sortedUserValues = userValues.toList.sortBy(_.timestamp)

    // Initialize first value at head of iterable and filter movements
    val firstValue = new StopCandidatePoint(sortedUserValues.head.vector)

    // Return detected stops only in form of string vector
    sortedUserValues
      // Visit each detected point and create list of StopCandidatePoint
      // (for details visit class desc.)
      .scanLeft((firstValue, ArrayBuffer[Double]()))(analyze)
      // Take window of 3 StopCandidatePoint's and determine stops in each groups
      .sliding(3).map(determineStop)
      // Return only points which were detected as stops
      .filter(_._2 == true)
      // Return result Vector
      .map(resultPair => Vector(resultPair._1.lat.toString,
                                resultPair._1.long.toString,
                                resultPair._1.id.toString,
                                resultPair._1.timestamp.toString)
      )
  }

  private def analyze(result: (StopCandidatePoint, ArrayBuffer[Double]),
                      current: DetectedPoint)
  : (StopCandidatePoint, ArrayBuffer[Double]) = {
    // Obtain parameters
    var lastPoint = result._1
    var durationsSlidingWindow = result._2

    if (durationsSlidingWindow.isEmpty) {
      durationsSlidingWindow += maxDuration
      // This is first element, pass as it was
      (lastPoint, durationsSlidingWindow)
    } else {
      // Determine duration between previous and current point
      val timeDifference = determineTimeDifference(
        lastPoint.timestamp, current.timestamp)
      durationsSlidingWindow += timeDifference

      // Determine mobility indexes and new duration sliding window
      val determinationResult = determineSlidingWindow(durationsSlidingWindow)
      val updatedSlidingWindow = determinationResult._1
      val mobilityIndex = determinationResult._2

      // Determine what is the behaviour type
      val distance = determineDistance(
        lastPoint.lat,
        lastPoint.long,
        current.lat,
        current.long)
      val speed = determineSpeed(distance, timeDifference)
      val behaviourType = determineBehaviour(distance, speed)

      // Add to feedback loop lists
      var resultPoint = new StopCandidatePoint(current)
      resultPoint.mobilityIndex = mobilityIndex
      resultPoint.behaviourType = behaviourType
      (resultPoint, updatedSlidingWindow)
    }

  }

  private def determineStop(window: List[(StopCandidatePoint, ArrayBuffer[Double])])
  : (DetectedPoint, Boolean) = {
    if (window.size == 2) {
      // If window is equal to two, there is no stop
      // since both stop candidates are the same points
      val currentResult = window(1)
      (currentResult._1, false)
    } else {
      val previousResult = window(0)
      val currentResult = window(1)
      val nextResult = window(2)

      if (currentResult._1.behaviourType == BehaviourType.PossibleStop ||
        currentResult._1.behaviourType == BehaviourType.Stop) {
        (currentResult._1, true)
      } else {
        (currentResult._1, false)
      }
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

      // Increase total duration and check
      if (totalDuration + current < slidingWindowThreshold) {
        totalDuration += current
        mobilityIndex += 1.0 / current
        newDurationsList += current
      }

      (newDurationsList, mobilityIndex, totalDuration)
    }
    }
  }

  private def determineBehaviour(distance: Double, speed: Double): BehaviourType.Type = {
    var result = BehaviourType.Travel
    if (speed < maxTransportSpeed) {
      if (speed < minWalkSpeedHuman && distance < maxWalkDistanceHuman) {
        result = BehaviourType.Stop
      }
      else if (speed > minWalkSpeedHuman && distance < maxWalkDistanceHuman) {
        result = BehaviourType.PossibleTravel
      }
      else if (speed < maxWalkSpeedHuman) {
        result = BehaviourType.PossibleStop
      }
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

