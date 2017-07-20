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

class CreateGraph() extends Serializable {

  def graphOperations(data: RDD[(String, Int)], spark: SparkSession): DataFrame = {

    val partitions = 2
    import spark.implicits._
    val edgeData  = data
      .groupBy(a => a._1).values
      .flatMap(mapEdge).groupBy(a => (a._1,a._2)).values.flatMap(tripCount)
      .map { line =>
        Edge(line._1.toInt, line._2.toInt, line._3.toString)
      }.cache()

    var graphRDD = Graph.fromEdges(edgeData , defaultValue = 1)

    val indegreeDF = graphRDD.inDegrees.toDF("vertexId","Indegrees")
      .coalesce(partitions).cache()

    val outdegreeDF = graphRDD.outDegrees.toDF("id","Outdegrees")
      .coalesce(partitions).cache()

    val alldegreesDF = outdegreeDF
      .join(indegreeDF, indegreeDF("vertexId") === outdegreeDF("id"), "full_outer")
      .withColumn("ClusterID",
        when($"vertexId".isNull, $"id")
          .otherwise($"vertexId"))
      .drop("vertexId","id")
      .coalesce(partitions).cache()

    var collVertIn  =
      graphRDD.collectNeighborIds(EdgeDirection.In)
        .map(e => (e._1,e._2.mkString(",")))
        .toDF("VID", "NeighborsIN")
        .coalesce(partitions).cache()

    var collVertOut  =
      graphRDD.collectNeighborIds(EdgeDirection.Out)
        .map(e => (e._1,e._2.mkString(",")))
        .toDF("VertID", "NeighborsOUT")
        .coalesce(partitions).cache()

    val dfneighbors = collVertIn
      .join(collVertOut, collVertOut("VertID")===collVertIn("VID"), "full_outer")
      .withColumn("VertexId",
        when($"VID".isNull, $"VertID")
          .otherwise($"VID"))
      .drop("VID","VertID")
      .coalesce(partitions).cache()

    val dfJoin2 = dfneighbors
      .join(alldegreesDF, dfneighbors("VertexId") === alldegreesDF("ClusterID"), "full_outer")
      .withColumn("Cluster#",
        when($"VertexId".isNull, $"ClusterID")
          .otherwise($"VertexId"))
      .drop("VertexId","ClusterID")
      .coalesce(partitions).cache()

    val PageRank = graphRDD.pageRank(0.0001).vertices
      .sortBy(_._2,ascending = false)
      .toDF("VertexId", "PageRank")
      .coalesce(partitions).cache()

    val dfJoin3 = PageRank
      .join(dfJoin2, PageRank("VertexId")===dfJoin2("Cluster#"), "full_outer")
      .withColumn("ClusterID",
        when($"VertexId".isNull, $"Cluster#")
          .otherwise($"VertexId"))
      .drop("VertexId","Cluster#")
      .na.fill(0,Seq("Indegrees"))
      .na.fill(0,Seq("Outdegrees"))

    dfJoin3
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