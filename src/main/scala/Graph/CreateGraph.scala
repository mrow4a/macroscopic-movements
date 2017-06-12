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
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
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
        (p(0), p(3))               // selecting only User ID and Cluster ID
      }.filter(_._2.toInt != 0)   // All lines with Cluster Id = 0 removed

   /*   val userClusterPts = data.map{ t =>
        val p = t.split(",")
        (p(0), p(1), p(2), p(3))
      }
       userClusterPts.coalesce(1).sortBy(_._1, ascending = false)
         .saveAsTextFile("resource/UserClusterPoints")   */

      // creates a RDD with avg Lat, Long for every cluster
      val vertexLatLong = data.map { t =>
        val p = t.split(",")
        (p(1), p(2), p(3))
      }.filter(_._3.toInt != 0)
       .groupBy(a=> (a._3)).values
      val avgVal = vertexLatLong.flatMap(avgLatLong)
     //  vertexLatLong.saveAsTextFile("resources/VertexLatLong")
      avgVal.coalesce(1).saveAsTextFile("resources/AvgLatLong")

        val userData = newTransactionsPair.groupBy(a => a._1).values     // without userId
        .flatMap(mapEdge)
        .filter(_._1.nonEmpty)
        .cache()      // -- will use this for tripcount() as well as other calculations.

      // with UserId. edges for each user:
        val newUserData = newTransactionsPair.groupBy(a => a._1)
        .values.flatMap(mapEdgePerUser).filter(_._1.nonEmpty)
        .sortBy(_._3, ascending = false)
        .groupBy(a => a._3)
        .values.map(_.toList)    // .mkstring  // check if python recognises this file or not

      // edges for each individual user. now to plot it somehow
    //   newUserData.coalesce(1).saveAsTextFile("resources/newUserData").toString

      // two hop count edges

  //  val twoHopEdges = userData.groupBy(a => a._1).values.saveAsTextFile("resources/twoHopEdges")

     var parsedData = userData.groupBy(a => (a._1,a._2)).values.flatMap(tripCount)

     //  parsedData.saveAsTextFile("resources/parsedData")
      var filePath = "resources/edges"
    //  userData.coalesce(1).saveAsTextFile(filePath)

      val edges: RDD[Edge[String]] = parsedData.map { line =>
        Edge(line._1.toInt, line._2.toInt, line._3.toString)
      }

    //  edges.coalesce(1).saveAsTextFile(filePath)

      var userGraph = Graph.fromEdges(edges , defaultValue = 1)


     // userGraph.triplets.saveAsTextFile("resources/triplet")
     // userGraph.vertices.saveAsTextFile("resources/vertices")

      // Popular Routes: edges with max property value.
   //  val maxAttr = userGraph.edges.map(_.attr).max
   //  val maxEdges = userGraph.edges.filter(_.attr == maxAttr )
   //  maxEdges.saveAsTextFile("resources/PopularRoutes")

     // Popular Destinations: High Indegree.
   // val popularVertices = userGraph.inDegrees.filter(_._2.toInt > 10)
    // popularVertices.saveAsTextFile("resources/PopularDestinations")

    // connected componenets -- Compute the connected component membership of each vertex and
    // return a graph with the vertex value containing the lowest vertex id
    // in the connected component containing that vertex.

    // val components = userGraph.connectedComponents()
    // components.triplets.saveAsTextFile("resources/CompnentTriplet")
 //   components.vertices.saveAsTextFile("resources/Componentvertices")
val newSubGraph = userGraph.subgraph()
// newSubGraph.vertices.saveAsTextFile("resources/SubGraphVertices")
   //   newSubGraph.edges.saveAsTextFile("resources/SubGraphEdges")

    val newGraph = userGraph.stronglyConnectedComponents(5)
    // newGraph.edges.coalesce(1).saveAsTextFile("resources/NewGraphEdges")
   // newGraph.vertices.coalesce(1).saveAsTextFile("resources/NewGraphVertices")
 /*  userGraph.vertices.leftJoin(components.vertices) {
        case (id, name, attr) => s"${id} is in component ${attr.get}"
      }.collect.foreach{ case (id, str) => println(str) } */
      // connections
       /* val nameCID = topicGraph.vertices.
        innerJoin(connectedComponentGraph.vertices) {
          (topicId, name, componentId) => (name, componentId)
        }  */

      context.stop()
    } // end of else
  } // end of object

  def mapEdge(user: Iterable[(String, String)])  :
  ArrayBuffer[(String, String)]
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

   def tripCount(data : Iterable[(String, String)]) :
   Iterable[(String, String, Int)] = {
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

   def avgLatLong(data : Iterable[(String,String,String)]) :
   Iterable[(String,String,String)] = {
     var countVal = 0
     var Lat = 0.0
     var Long = 0.0
     val resultAvg = data.foldLeft(ArrayBuffer[(String, String, String)]()) { (result, c) => {
       countVal +=1
       Lat += c._1.toDouble
       Long += c._2.toDouble
       val temp = ((Lat/countVal).toString , (Long/countVal).toString , c._3)
       result += temp
     }
     }.last      // end of foldLeft
     val resultFinal = (resultAvg._1 , resultAvg._2, resultAvg._3)
     List(resultFinal)
   }



  def mapEdgePerUser(user: Iterable[(String, String)])  :
  ArrayBuffer[(String, String,String)]
  = {
    user.foldLeft(ArrayBuffer[(String, String,String)]()) { (result, c) => {
      if (result.nonEmpty) {
        val head = result.last
        if(c._2 != head._2) {
          val toWrite = (head._2,c._2,c._1)
          result += toWrite
        }
      }
      else
      {
        val temp = ("",c._2,c._1)
        result += temp
      }
      result
    } // end of foldLeft
    }
  }// end of function
}



