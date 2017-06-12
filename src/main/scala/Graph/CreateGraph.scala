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
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by ananya on 23.05.17.
  */
class CreateGraph(sc: SparkContext) {

}


object CreateGraph {
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
      val job = new CreateGraph(context)

      val data = context.textFile(src)

      val newTransactionsPair = data.map { t =>
        val p = t.split(",")
        (p(2), p(3))
      }.filter(_._2.toInt != 0)   // All lines with Cluster Id = 0 removed.

      val userData = newTransactionsPair.groupBy(a => a._1).values
        .flatMap(mapEdge)
        .filter(_._1.nonEmpty)
        .cache()      // -- will use this for tripcount() as well as other calculations.

      var parsedData = userData
        .groupBy(a => (a._1,a._2)).values.flatMap(tripCount)

       parsedData.saveAsTextFile("resources/parsedData")
      var filePath = "resources/edges"
    //  userData.coalesce(1).saveAsTextFile(filePath)

      val edges: RDD[Edge[String]] = parsedData.map { line =>
        Edge(line._1.toInt, line._2.toInt, line._3.toString)
      }

      edges.saveAsTextFile(filePath)

      var userGraph = Graph.fromEdges(edges , defaultValue = 1)
      userGraph.triplets.saveAsTextFile("resources/triplet")

      context.stop()
    }
  }

  def mapEdge(user: Iterable[(String, String)])  : ArrayBuffer[(String, String)]
  = {
    user.foldLeft(ArrayBuffer[(String, String)]()) { (result, c) => {
      if (result.nonEmpty) {
        val head = result.last
        if(c._2 != head._2) {
          val toWrite = (head._2,c._2)
          result += toWrite
        }
      }
      else
      {
        val temp = ("",c._2)
        result += temp
      }
      result
    } // end of foldLeft
    }
  }// end of function

   def tripCount(data : Iterable[(String, String)]) : Iterable[(String, String,Int)] = {
    var count = 0
    val resultMax = data.foldLeft(ArrayBuffer[(String,String,Int)]()) { (result,c) => {
        count = count + 1
        val temp = (c._1, c._2, count)
        result += temp
        result
    }}.max  // end of foldLeft */
    val resultFinal = (resultMax._1,resultMax._2,resultMax._3)
    List(resultFinal)
  }   // end of funtion
}



