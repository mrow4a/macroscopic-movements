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

case class DetectedPoint(vector: Vector[String]){

  def timestamp: Int = getTimestamp(vector(0), vector(1))
  def id: String = vector(2)
  def lat: Double = vector(3).toDouble
  def long: Double = vector(4).toDouble

  def getTimestamp(dayOfWeek: String, timeOfDay: String) : Int = {
    val splitTimestamp = timeOfDay.split(':')
    dayOfWeek.toInt * 86400 +          // convert day number to seconds
      splitTimestamp(0).toInt * 3600 + // convert hours number to seconds
      splitTimestamp(1).toInt * 60 +   // convert minutes number to seconds
      splitTimestamp(2).toInt          // get seconds
  }

}

class Movement(start: DetectedPoint, end: DetectedPoint){

  def this() = this(DetectedPoint(Vector()), DetectedPoint(Vector()))

  /* Constants */
  def minDuration: Double = 0.0001 // default to very small number in seconds
  def maxDuration: Double = 86400 // default to 24h in seconds

  def startPoint: DetectedPoint = start
  def endPoint: DetectedPoint = end

  private var distance: Double = -1

  override def toString(): String = {
    "START: " + startPoint + " END: " + endPoint + " DIS: " + distance + " DUR: " + getDuration
  }

  def getDistance: Double = {
    if (distance == -1) {
      distance =
        determineDistance(startPoint.lat, startPoint.long, endPoint.lat, endPoint.long)
    }
    distance
  }

  def getDuration: Double = {
    var pointsDuration = (endPoint.timestamp - startPoint.timestamp).toDouble
    if (pointsDuration == 0) {
      minDuration
    } else {
      pointsDuration
    }
  }

  def getSpeed: Double = {
    getDistance/getDuration
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
}
