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

case class DBSCANPoint(vector: Vector[String]) {

  // 1st and 2nd are reserved
  def timestamp: Int = getTimestamp(vector(0), vector(1))
  def id: String = vector(2) // user id
  def x: Double = vector(3).toDouble // lat
  def y: Double = vector(4).toDouble // long
  def duration: Double = vector(5).toDouble // long
  def areaID: Int = vector(6).toInt // area id
  def areaEps: Double = vector(7).toDouble // area id

  def distanceSquared(other: DBSCANPoint): Double = {
    val dx = other.x - x
    val dy = other.y - y
    (dx * dx) + (dy * dy)
  }

  def getTimestamp(dayOfWeek: String, timeOfDay: String) : Int = {
    val splitTimestamp = timeOfDay.split(':')
    dayOfWeek.toInt * 86400 +          // convert day number to seconds
      splitTimestamp(0).toInt * 3600 + // convert hours number to seconds
      splitTimestamp(1).toInt * 60 +   // convert minutes number to seconds
      splitTimestamp(2).toInt          // get seconds
  }
}
