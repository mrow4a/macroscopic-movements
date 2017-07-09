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
    * STOP DETECTION SPECIFIC
    * @param durationsWindowSize
    * @param mobilityIndexThreshold
    * @param stopAccuracyDistance
    * @param stopAccuracySpeed
    *
    * ANOMALY FILTERING
    * @param minimumFlightSpeed
    * @param minimumFlightDistance
    * @param minimumAccuracyDistance
    * @param minimumAccuracyDuration
    */
  def filter(
      data: RDD[Vector[String]],
      durationsWindowSize: Double,
      mobilityIndexThreshold: Double,
      stopAccuracyDistance: Double,
      stopAccuracySpeed: Double,
      minimumFlightSpeed: Double,
      minimumFlightDistance: Double,
      minimumAccuracyDistance: Double,
      minimumAccuracyDuration: Double): RDD[Vector[String]] = {

    new StopDetection(
      durationsWindowSize,
      mobilityIndexThreshold,
      stopAccuracyDistance,
      stopAccuracySpeed,
      minimumFlightSpeed,
      minimumFlightDistance,
      minimumAccuracyDistance,
      minimumAccuracyDuration).filter(data)
  }
}

class StopDetection private(
      val durationsWindowSize: Double,
      val mobilityIndexThreshold: Double,
      val stopAccuracyDistance: Double,
      val stopAccuracySpeed: Double,
      val minimumFlightSpeed: Double,
      val minimumFlightDistance: Double,
      val minimumAccuracyDistance: Double,
      val minimumAccuracyDuration: Double)
  extends Serializable {

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
    val firstMovement = new Movement()
    userValues
      // Ensure that values are sorted by timestamp
      .toList.sortBy(_.timestamp)
      // Create Movement from two DetectedPoints
      .sliding(2).filter(_.size==2).map(getMovement)
      // Filter movement anomalies
      .filter(filterAnomalies)
      // Determine sliding window and remove firstMovement
      .scanLeft((firstMovement, 0.0, ArrayBuffer[Double]()))(determineSlidingWindow).drop(1)
      // Filter movements
      .map(pair => (pair._1, pair._2))
      .filter(filterMovements)
      // In this version, stop point is in movement starting point
      .map(stop => Vector(stop._1.startPoint.lat.toString, stop._1.startPoint.long.toString,
          stop._1.startPoint.id.toString,
          stop._1.startPoint.timestamp.toString,
          stop._1.getDuration.toString)
      )
  }

  private def filterMovements(pair: (Movement, Double))
  : Boolean = {
    val movement = pair._1
    val distance = movement.getDistance
    val speed = movement.getSpeed
    val mobilityIndex = pair._2

    if ((distance < stopAccuracyDistance && speed < stopAccuracySpeed)
      || (distance > stopAccuracyDistance && mobilityIndex < mobilityIndexThreshold)){
      // println(true, pair)
      true
    } else {
      // println(false, pair)
      false
    }
  }

  private def determineSlidingWindow(result: (Movement, Double, ArrayBuffer[Double]),
                      current: Movement)
  : (Movement, Double, ArrayBuffer[Double]) = {
    // Obtain parameters
    var durationsList = result._3

    // Update list with duration of current point
    durationsList += current.getDuration

    val mobilityIndex = durationsList
      .foldRight((0.0, 0.0)) {
        (current, result) => {
          // Obtain parameters
          var mobilityIndex = result._1
          var totalDuration = result._2

          totalDuration += current // increase sum of past durations
          // Check if sum of duration did not exceed the maximum sliding window duration
          if (totalDuration < durationsWindowSize) {
            mobilityIndex += 1/current
          }

          (mobilityIndex, totalDuration)
        }
      }
      ._1

    (current, mobilityIndex, durationsList)
  }

  def determineIndex(result: (Double, ArrayBuffer[Double]))
  : (Double, ArrayBuffer[Double]) = {
    result
  }

  private def getMovement(window: List[DetectedPoint])
  : Movement = {
    val startPoint = window(0)
    val endPoint = window(1)
    // Determine duration, distance and speed between previous and current point

    new Movement(startPoint, endPoint)
  }

  private def filterAnomalies(movement: Movement)
  : Boolean = {
    val distance = movement.getDistance
    val duration = movement.getDuration
    val speed = movement.getSpeed

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
}
