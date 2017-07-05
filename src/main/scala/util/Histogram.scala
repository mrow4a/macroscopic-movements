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
  * Created by gabri on 2017-06-07.
  */

//import org.sameersingh.scalaplot.Implicits._

object Histogram {

  def main(args: Array[String]) {
    /*   val events = cmsdata.EventIterator()

       val histogram = Bin(10, 0, 100, { event: Event => event.met.pt })

       for (event <- events.take(1000))
         histogram.fill(event)

       println(histogram)
       // histogram.println
   */


    val x = 0.0 until 2.0 * math.Pi by 0.1
//    output(ASCII, xyChart(x ->(math.sin(_), math.cos(_))))
  }

}
