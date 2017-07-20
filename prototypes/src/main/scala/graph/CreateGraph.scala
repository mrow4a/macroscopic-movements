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

package graph
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.graphx._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.functions.{avg, explode}
import org.apache.spark.sql.functions._
import org.graphframes._

// SPARK_HOME/bin/spark-shell --packages dmarcous:spark-betweenness:1.0-s_2.10
/**
  * Created by ananya on 23.05.17.
  */


class CreateGraph() extends Serializable {

  def graphOperations(data: RDD[(String, Int)], spark: SparkSession): DataFrame = {

    import spark.implicits._
    val edgeData  = data.groupBy(a => a._1).values
      .flatMap(mapEdge).groupBy(a => (a._1,a._2)).values.flatMap(tripCount)
    .map { line =>
      Edge(line._1.toInt, line._2.toInt, line._3.toString)
    }

     var graphRDD = Graph.fromEdges(edgeData , defaultValue = 1)

    val indegreeDF = graphRDD.inDegrees.toDF("vertexId","Indegrees")

    val outdegreeDF = graphRDD.outDegrees.toDF("id","Outdegrees")


    val alldegreesDF = outdegreeDF
      .join(indegreeDF, indegreeDF("vertexId") === outdegreeDF("id"), "full_outer")
      .withColumn("ClusterID",
        when($"vertexId".isNull, $"id")
          .otherwise($"vertexId"))
      .drop("vertexId","id")

    var collVertIn  =
      graphRDD.collectNeighborIds(EdgeDirection.In)
        .map(e => (e._1,e._2.mkString(",")))
        .toDF("VID", "NeighborsIN")


    var collVertOut  =
      graphRDD.collectNeighborIds(EdgeDirection.Out)
        .map(e => (e._1,e._2.mkString(",")))
        .toDF("VertID", "NeighborsOUT")

  //  var temp = collVertIn.fullOuterJoin(collVertOut)
    // converting connected neighbors RDD to DF
    val dfneighbors = collVertIn.join(collVertOut, collVertOut("VertID")===collVertIn("VID"), "full_outer")
      .withColumn("VertexId",
      when($"VID".isNull, $"VertID")
        .otherwise($"VID"))
      .drop("VID","VertID")
//      .map{ u => (u._1,u._2._1.toList.toString, u._2._2.toList.toString)}
//      .toDF("VertexId", "NeighborsIN", "NeighborsOut")


    // joining dfJoinLatLongDeg with neighbors DF
    val dfJoin2 = dfneighbors
      .join(alldegreesDF, dfneighbors("VertexId") === alldegreesDF("ClusterID"), "full_outer")
      .withColumn("Cluster#",
        when($"VertexId".isNull, $"ClusterID")
          .otherwise($"VertexId"))
      .drop("VertexId","ClusterID")

     val PageRank = graphRDD.pageRank(0.0001).vertices.sortBy(_._2,ascending = false)
       .toDF("VertexId", "PageRank")
   // val pageRankDF = graphDF.pageRank.resetProbability(0.15).maxIter(1).run()
  //  val vertexPG = pageRankDF.vertices.select("id", "pagerank")

    val dfJoin3 = PageRank
      .join(dfJoin2, PageRank("VertexId")===dfJoin2("Cluster#"), "full_outer")
      .withColumn("ClusterID",
        when($"VertexId".isNull, $"Cluster#")
          .otherwise($"VertexId"))
      .drop("VertexId","Cluster#")
      .na.fill(0,Seq("Indegrees"))
      .na.fill(0,Seq("Outdegrees"))
 //     .coalesce(1).write.option("header", "true").csv("resources/sample_file")


    // val components = userGraph.connectedComponents()

    // val newGraph = userGraph.stronglyConnectedComponents(5)
    dfJoin3
  }    // end of function

  def tripCount(data : Iterable[(Int, Int)]) :
  Iterable[(Int, Int, Int)] = {
    var count = 0
    val resultMax = data.foldLeft(ArrayBuffer[(Int,Int,Int)]()) { (result,c) => {
      count = count + 1
      val temp = (c._1, c._2, count)
      result += temp
      result
    }}.max  // end of foldLeft */
    val resultFinal = (resultMax._1,resultMax._2,resultMax._3)
    List(resultFinal)
  }   // end of funtion

  def mapEdge(user: Iterable[(String,Int)])  :
  Iterable[(Int,Int)]
  = {
    user.foldLeft(ArrayBuffer[(Int,Int)]()) { (result, c) => {
      if (result.nonEmpty) {
        val head = result.last
        if(c._2 != head._2)  {
          val toWrite = (head._2,c._2)
          result += toWrite
        }
      }
      else
      {
        val temp = (0,c._2)
        result += temp
      }
      result
    } // end of foldLeft
    }.drop(1)
  }// end of function

}   // end of class


object CreateGraph {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.exit(1)
    }
    else {
      val src = args(0)
      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.master", "local")
        .getOrCreate()


      val job = new CreateGraph()
      val data = spark.sparkContext.textFile(src)
      val parsedData = data.map(line => line.split(","))
        .filter(_(1).toInt != 0).map(s => (s(0), s(1).toInt))
      val result = job.graphOperations(parsedData, spark)
      result.show(false)
      spark.stop()

    } // end of else
  } // end of main
}


