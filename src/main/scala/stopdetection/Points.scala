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

object BehaviourType extends Enumeration {
  type Type = Value

  val Stop = Value(0)
  val PossibleStop = Value(1)
  val PossibleTravel = Value(2)
  val Travel = Value(3)
}

class StopCandidatePoint(vector: Vector[String]) extends DetectedPoint(vector){

  def this(point: DetectedPoint) = this(point.vector)

  var mobilityIndex: Double = -1
  var speed: Double = -1
  var distance: Double = -1
  var duration: Double = -1
  var behaviourType: BehaviourType.Type = BehaviourType.Travel

  override def toString(): String = {
    vector + " " +mobilityIndex + " " + behaviourType
  }
}
