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

package Graph
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ananya on 04.07.17.
  */
class GraphAnalysis(sc: SparkContext) {

}
object GraphAnalysis {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.exit(1)
    }
    else {
      val src = args(0)
      val conf = new SparkConf()
      conf.setAppName(s"CreateVertex()")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.setMaster("local[2]").set("spark.executor.memory", "1g")
      val context = new SparkContext(conf)
      val job = new GraphAnalysis(context)

      val data = context.textFile(src).map { t =>
        val p = t.split(",")
        (p(0), p(3))}.cache()

      val ClusterCount= data.filter(_._2.toInt !=0).groupBy(_._1).values
        .flatMap(CountClusters).coalesce(1)
        .sortBy(_._1,ascending = false)    // .saveAsTextFile("resources/ClusterCount")

      val OutlierCount = data.filter(_._2.toInt ==0).groupBy(_._1).values
        .flatMap(CountClusters).coalesce(1)
        .sortBy(_._1,ascending = false)
        .saveAsTextFile("resources/OutlierCount")
     // ClusterCount.fullOuterJoin(OutlierCount).sortBy(_._1,ascending = false)
           // .saveAsTextFile("resources/MergedFile")
 context.stop()
  } // end of else
 } // end of main


def CountClusters(data: Iterable[(String, String)])  :
Iterable[(String, String)]
= {
  var countFlag = 0
  val FinalCount = data.foldLeft(ArrayBuffer[(String, String)]()) { (result,c) => {
    countFlag = countFlag + 1
    val temp = (c._1.toString, countFlag.toString)
    result += temp
    result
  }}.last  // end of foldLeft */
  val resultFinal = (FinalCount._1,FinalCount._2)
  List(resultFinal)
}// end of function

  def formatFile(data: Iterable[(String, String)])  :
  Iterable[(String, String)]
  = {
    var countFlag = 0
    val FinalCount = data.foldLeft(ArrayBuffer[(String,String)]()) { (result,c) => {
      countFlag = countFlag + 1
      val temp = (c._1.toString, countFlag.toString)
      result += temp
      result
    }}.last  // end of foldLeft */
    val resultFinal = (FinalCount._1,FinalCount._2)
    List(resultFinal)
  }// end of function



  // unwanted code //
  /*
  var vertexSet : RDD[(VertexId,String)]= avgVal.map { line =>
    (line._3.toInt, ("" + line._1.toString + "," + line._2.toString + ""))
  }
*/
} // end of object
