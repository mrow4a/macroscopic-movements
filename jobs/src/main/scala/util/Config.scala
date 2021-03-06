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
package util

/**
  * Created by gabri on 2017-07-03.
  */
object Config {

  val durationsSlidingWindowSize = 1800.0
  // By default 20 minutes
  val mobilityIndexThreshold = 0.0017
  // Mobility Index Threshold used to determine mobility patterns
  val distanceThreshold = 1000
  // meters
  val speedThreshold = 1.4 // m/s

  // Parameters for anomaly filtering
  val minimumFlightSpeed = 83
  // Filter all speeds above 300 km/h
  val minimumFlightDistance = 100000
  // Filter all speeds above 300 km/h with distances over 100km
  val minimumAccuracyDistance = 100
  // Filter all points within distance of 100m, anomalies
  val minimumAccuracyDuration = 100 // Filter all points within duration of 100s, anomalies

  val maxPointsPerPartition = 50000
  val eps = 0.005

  val unknownAreaID = 0

  object innerBerlin extends Enumeration {
    val (xMin, xMax, yMin, yMax, id, eps, minPts) = (52.4425, 13.2582, 52.5647, 13.4818, 1, 0.001, 5)
  }

  object middleBerlin extends Enumeration {
    val (xMin, xMax, yMin, yMax, id, eps, minPts) = (52.3446, 13.0168, 52.6375, 13.6603, 2, 0.003, 5)
  }

  object outsideBerlin extends Enumeration {
    // whole germany     (47.23  , 5.03   , 55.04  , 15.23  , 3, 0.005, 20)
    // whole berlin area (52.0102, 11.2830, 53.0214, 13.9389, 3, 0.005, 5)
    val (xMin, xMax, yMin, yMax, id, eps, minPts) = (52.137, 12.695, 52.899, 14.172, 3, 0.005, 5)
  }
}
