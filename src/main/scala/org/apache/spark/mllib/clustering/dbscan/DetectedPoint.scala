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
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.clustering.dbscan.BehaviourType.Type

// TODO: use this class instead of DBSCANpoint
case class DetectedPoint(vector: Vector[String]){

  def dayOfWeek: Int = vector(0).toInt
  def secondsOfDay: Int = getSecondsSinceMidnight(vector(1).toString)
  def timestamp: String = vector(1).toString
  def id: String = vector(2).toString
  def x: Double = vector(3).toDouble // TODO: x,y should be first and second parameters
  def y: Double = vector(4).toDouble


  def getSecondsSinceMidnight(timestamp: String) : Int = {
    val splitTimestamp = timestamp.split(':')
    splitTimestamp(0).toInt * 3600 + splitTimestamp(1).toInt * 60 + splitTimestamp(2).toInt
  }

  override def toString: String = {
    id + " " + dayOfWeek +  " " +timestamp + " " + y + " " + x
  }

  def distanceSquared(other: DetectedPoint): Double = {
    val dx = other.x - x
    val dy = other.y - y
    (dx * dx) + (dy * dy)
  }

}

object BehaviourType extends Enumeration {
  type Type = Value

  val Stop = Value(0)
  val PossibleStop = Value(1)
  val PossibleTravel = Value(2)
  val Travel = Value(3)
}

case class StopCandidatePoint(dP: DetectedPoint,
                              mI: Double = 0,
                              bT: BehaviourType.Type = BehaviourType.Travel){
  def detectedPoint: DetectedPoint = dP
  def mobilityIndex: Double = mI
  def behaviourType: Type = bT

  override def toString: String = {
    detectedPoint + " " + mobilityIndex +  " " + behaviourType
  }
}

