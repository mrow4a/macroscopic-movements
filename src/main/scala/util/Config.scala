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

  val durationsSlidingWindowSize = 2400.0
  // By default 40 minutes
  val stopCertaintyMaxDistance = 1500.0
  // By default max walking distance for human
  val stopCertaintyMaxSpeed = 0.833
  // By default min human walking speed
  val travelCertaintyMinSpeed = 1.4
  val maxPointsPerPartition = 10000
  val eps = 0.001
  val minPoints = 5

  object innerBerlin extends Enumeration {
    val (xMin, xMax, yMin, yMax) = (52.4425, 13.2582, 52.5647, 13.4818)
  }
  object middleBerlin extends Enumeration {
    val (xMin, xMax, yMin, yMax) = (52.3446, 13.0168, 52.6375, 13.6603)
  }
}
