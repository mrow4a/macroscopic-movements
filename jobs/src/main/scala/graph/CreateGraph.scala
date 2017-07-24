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
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._

import scala.math.Ordering

class CreateGraph() extends Serializable {

  def graphOperations(data: RDD[(String, Int)], spark: SparkSession): DataFrame = {

    val partitions = 2
    val edgeWeight = 1
    import spark.implicits._
    val edgeData  = data
      .groupBy(a => a._1).values
      .flatMap(mapEdge)
      // TODO: Include TripCount as an attribute
      // -> this may affect PageRank because duplicate edges will dissapear
      // .groupBy(a => (a._1,a._2)).values.flatMap(tripCount)
      .map { line =>
        Edge(line._1.toInt, line._2.toInt, edgeWeight)
      }
    /*  Section: Simple Graph : this is for creating a graph based on tripcounts
      // Create simple directed graph  - i.e. without parallel edges
      // create edges for simple directed graph
      val simpleEdgeData  = data
        .groupBy(a => a._1).values
        .flatMap(mapEdge)
          .groupBy(a => (a._1,a._2)).values.flatMap(tripCount)
        .map { line =>
        Edge(line._1.toInt, line._2.toInt, tripCount)
      }
     // create simple graph
     var simpleGraphRDD = Graph.fromEdges(simpleEdgeData , defaultValue = 1)

    // obtain the most important edges as per trip count
    val topEdges = simpleGraphRDD.edges.top(5)(Ordering.by(_.attr))
  */

    // Create drected multi-graph  - i.e. the graph will contain parallel edges.
    var graphRDD = Graph.fromEdges(edgeData , defaultValue = 1)

    // Get Edges IN
    var collVertIn  =
      graphRDD.collectNeighborIds(EdgeDirection.In)
        .map(e => (e._1,e._2.mkString(",")))
        .toDF("ClusterID", "NeighborsIN")
        .cache()

    // Get Edges OUT
    var collVertOut  =
      graphRDD.collectNeighborIds(EdgeDirection.Out)
        .map(e => (e._1,e._2.mkString(",")))
        .toDF("ClusterID", "NeighborsOUT")

    // Calculate InDegrees
    val indegreeDF = graphRDD.inDegrees.toDF("ClusterID","Indegrees")

    // Calculate OutDegrees
    val outdegreeDF = graphRDD.outDegrees.toDF("ClusterID","Outdegrees")
      // Cache since all join will join with it

    // Get PageRank
    val pageRank = graphRDD.pageRank(0.0001).vertices
      .sortBy(_._2,ascending = false)
      .toDF("ClusterID", "PageRank")

    val joinedTables = collVertIn
      // Join with NeighborsOut
      .join(collVertOut, collVertOut("ClusterID") <=> collVertIn("ClusterID"), "full_outer")
      .drop(collVertOut("ClusterID"))
      // Join with InDegrees
      .join(indegreeDF, indegreeDF("ClusterID") <=> collVertIn("ClusterID"), "full_outer")
      .drop(indegreeDF("ClusterID"))
      // Join with OutDegrees
      .join(outdegreeDF, outdegreeDF("ClusterID") <=> collVertIn("ClusterID"), "full_outer")
      .drop(outdegreeDF("ClusterID"))
      // Join with PageRank
      .join(pageRank, pageRank("ClusterID") <=> collVertIn("ClusterID"), "full_outer")
      .drop(pageRank("ClusterID"))
      // Replace null with 0
      .na.fill(0,Seq("Indegrees"))
      .na.fill(0,Seq("Outdegrees"))

    joinedTables
  }

  def tripCount(data : Iterable[(Int, Int)]) :
  Iterable[(Int, Int, Int)] = {
    var count = 0
    val resultMax = data.foldLeft(ArrayBuffer[(Int,Int,Int)]()) { (result,c) => {
      count = count + 1
      val temp = (c._1, c._2, count)
      result += temp
      result
    }}.max
    val resultFinal = (resultMax._1,resultMax._2,resultMax._3)
    List(resultFinal)
  }

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
    }
    }.drop(1)
  }

}

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

    }
  }
}