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

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Top level method for calling StopDetection
  */
object StopDetection {

  /**
    * TODO: it might need some configuration parameters
    */
  def filter(
             data: RDD[DetectedPoint]): RDD[Vector] = {

    new StopDetection().filter(data)

  }

}

/**
  * TODO:
  */
class StopDetection private ()

  extends Serializable {

  /**
    * This function filters all DetectedPoints and
    * return Vector with (0)->Latitude and (1)->Longitude
    */
  private def filter(vectors: RDD[DetectedPoint]): RDD[Vector] = {
    vectors
      .filter(isStop)
      .map(detectedPoint => Vectors.dense(detectedPoint.lat, detectedPoint.long))
  }

  private def isStop(detectedPoint: DetectedPoint): Boolean = {
    // TODO: Write stop function
    true
  }
}

