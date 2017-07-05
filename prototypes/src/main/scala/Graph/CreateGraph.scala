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
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.functions.{avg, explode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
// import com.centrality.kBC.KBetweenness

// SPARK_HOME/bin/spark-shell --packages dmarcous:spark-betweenness:1.0-s_2.10
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

      // creates a RDD with avg Lat, Long for every cluster
      val vertexLatLong = data.map { t =>
        val p = t.split(",")
        (p(1), p(2), p(3))
      }.filter(_._3.toInt != 0)
       .groupBy(a=> (a._3)).values

      val avgVal = vertexLatLong.flatMap(avgLatLong)

        val userData = newTransactionsPair.groupBy(a => a._1).values
        .flatMap(mapEdge)
        .filter(_._1.nonEmpty)
        .cache()       // -- will use this for tripcount() as well as other calculations.


     var parsedData = userData.groupBy(a => (a._1,a._2)).values.flatMap(tripCount)


      val edges: RDD[Edge[String]] = parsedData.map { line =>
        Edge(line._1.toInt, line._2.toInt, line._3.toString)
      }

      var userGraph = Graph.fromEdges(edges , defaultValue = 1)

      var Indegrees = userGraph.inDegrees
      var Outdegrees = userGraph.outDegrees

      var AllDegrees = Indegrees.fullOuterJoin(Outdegrees).sortBy(_._1,ascending = false)

       var collVertIn : RDD[(VertexId,List[Long])] =
        userGraph.collectNeighborIds(EdgeDirection.In)
        .map(e => (e._1,e._2.toList))

      var collVertOut : RDD[(VertexId,List[Long])] =
      userGraph.collectNeighborIds(EdgeDirection.Out)
        .map(e => (e._1,e._2.toList))

      var temp = collVertIn.fullOuterJoin(collVertOut)

      val PageRank = userGraph.pageRank(0.0001).vertices.sortBy(_._2,ascending = false).coalesce(1)

    val sqlcontext = new SparkSession.Builder
      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

      import spark.implicits._

      // converting lat,long RDD to DF
      val dfVertexset = avgVal.toDF("Latitude","Longitude","VertexId")
        .sort("VertexId")

      // converting all degree RDD to DF
      val dfDG = AllDegrees.map{ u=> (u._1.toString,
        u._2._1.toList.toString, u._2._2.toList.toString)}
        .toDF("ClusterID", "Indegrees", "Outdegrees").sort("ClusterID")

      // joining all degree  DF with latlong DF
      val dfJoinLatLongDeg =  dfDG
         .join(dfVertexset, dfDG("ClusterID") === dfVertexset("VertexId"), "full_outer")
       .withColumn("Cluster#",
         when($"ClusterID".isNull, $"VertexId")
         .otherwise($"ClusterID"))
        .drop("ClusterID","VertexId")

      // converting connected neighbors RDD to DF
     val dfneighbors = temp
       .map{ u => (u._1,u._2._1.toList.toString, u._2._2.toList.toString)}
       .toDF("VertexId", "NeighborsIN", "NeighborsOut")

      // joining dfJoinLatLongDeg with neighbors DF
      val dfJoin2 = dfneighbors
        .join(dfJoinLatLongDeg, dfneighbors("VertexId") === dfJoinLatLongDeg("Cluster#"), "full_outer")
      .withColumn("ClusterID",
        when($"VertexId".isNull, $"Cluster#")
        .otherwise($"VertexId"))
       .drop("VertexId","Cluster#")

      // converting pagerank RDD to DF
       val dfPageRank = PageRank.toDF("VertexId", "PageRank")


        val dfJoin3 = dfPageRank
          .join(dfJoin2, dfPageRank("VertexId")===dfJoin2("ClusterID"), "full_outer")
        .withColumn("Cluster#",
          when($"VertexId".isNull, $"ClusterID")
           .otherwise($"VertexId"))
          .drop("VertexId","ClusterID")
          .coalesce(1).write.option("header", "true").csv("resources/sample_file")



       // println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString(“\n”))
       // val top15Places = PageRank.top(5)(Ordering.by(_).mkString("\n"))
      // println(top15Places)


      // Popular Routes: edges with max property value.
   //  val maxAttr = userGraph.edges.map(_.attr).max
   //  val maxEdges = userGraph.edges.filter(_.attr == maxAttr )

     // Popular Destinations: High Indegree.
   // val popularVertices = userGraph.inDegrees.filter(_._2.toInt > 10)

    // val components = userGraph.connectedComponents()

    // val newSubGraph = userGraph.subgraph()

     // val newGraph = userGraph.stronglyConnectedComponents(5)



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




}



