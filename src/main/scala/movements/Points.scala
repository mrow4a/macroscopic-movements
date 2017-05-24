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

package movements

import org.apache.spark.mllib.clustering.dbscan.DetectedPoint

object BehaviourType extends Enumeration {
  type Type = Value

  val Stop = Value(0)
  val PossibleStop = Value(1)
  val PossibleTravel = Value(2)
  val Travel = Value(3)
}

case class StopCandidatePoint(val dP: DetectedPoint,
                               val mI: Double = 0,
                               val bT: BehaviourType.Type = BehaviourType.Travel){
  def detectedPoint = dP
  def mobilityIndex = mI
  def behaviourType = bT

  override def toString(): String = {
    detectedPoint + " " + mobilityIndex +  " " + behaviourType
  }
}
