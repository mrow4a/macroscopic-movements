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
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkConf
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
import com.centrality.kBC.KBetweenness
import org.apache.spark.sql.Row
import org.graphframes._

// SPARK_HOME/bin/spark-shell --packages dmarcous:spark-betweenness:1.0-s_2.10
/**
  * Created by ananya on 23.05.17.
  */


class CreateGraph() {



  def graphOperations(filepath: String): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val basicDF = spark.read.csv(filepath).toDF("UserID", "Latitude", "Longitude", "VertexId")
      .where($"VertexId" =!= 0)    // All lines with Cluster Id = 0 removed

    val parsedDF = basicDF.selectExpr("UserId","cast(Latitude as float) Latitude",
      "cast(Longitude as float) Longitude",
      "VertexId")

    // getting average Latitude and LOngitude values
    val AvgLatLongDF = parsedDF.groupBy("VertexId").avg("Latitude","Longitude")

    // creating RDD with only User ID and Cluster ID
    val tempRDD   = parsedDF.select("UserId","VertexId").as[(String, String)].rdd

    // creating edges using the RDD and converting back to DF;
    // will use this for tripcount() as well as other calculations.
    val edgeData  = tempRDD.groupBy(a => a._1).values.flatMap(CreateGraph.mapEdge).filter(_._1.nonEmpty)
      .toDF("Source","Destination")

    val countEdgeDF = edgeData.selectExpr("cast(Source as integer) src"
      , "cast(Destination as integer) dst").groupBy("src", "dst").count()

    val graphDF = GraphFrame.fromEdges(countEdgeDF)

    val indegreeDF = graphDF.inDegrees.withColumnRenamed("id", "vertexId")

    val outdegreeDF = graphDF.outDegrees
    val alldegreesDF = outdegreeDF
      .join(indegreeDF, indegreeDF("vertexId") === outdegreeDF("id"), "full_outer")
      .withColumn("ClusterID",
        when($"vertexId".isNull, $"id")
          .otherwise($"vertexId"))
      .drop("vertexId","id")

    // joining all degree  DF with latlong DF
    val dfJoinLatLongDeg =  alldegreesDF
      .join(AvgLatLongDF, alldegreesDF("ClusterID") === AvgLatLongDF("VertexId"), "full_outer")
      .withColumn("Cluster#",
        when($"ClusterID".isNull, $"VertexId")
          .otherwise($"ClusterID"))
      .drop("ClusterID","VertexId")

    var graphRDD = graphDF.toGraphX

    var collVertIn : RDD[(VertexId,List[Long])] =
      graphRDD.collectNeighborIds(EdgeDirection.In)
        .map(e => (e._1,e._2.toList))

    var collVertOut : RDD[(VertexId,List[Long])] =
      graphRDD.collectNeighborIds(EdgeDirection.Out)
        .map(e => (e._1,e._2.toList))

    var temp = collVertIn.fullOuterJoin(collVertOut)
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

    // val PageRank = graphRDD.pageRank(0.0001).vertices.sortBy(_._2,ascending = false)
    val pageRankDF = graphDF.pageRank.resetProbability(0.15).maxIter(1).run()
    val vertexPG = pageRankDF.vertices.select("id", "pagerank")

    val dfJoin3 = vertexPG
      .join(dfJoin2, vertexPG("id")===dfJoin2("ClusterID"), "full_outer")
      .withColumn("Cluster#",
        when($"id".isNull, $"ClusterID")
          .otherwise($"id"))
      .drop("id","ClusterID")
      .coalesce(1).write.option("header", "true").csv("resources/sample_file")


    // val components = userGraph.connectedComponents()

    // val newGraph = userGraph.stronglyConnectedComponents(5)

    spark.stop()
  }    // end of function

}   // end of class


object CreateGraph {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.exit(1)
    }
    else {
      val src = args(0)
      // val conf = new SparkConf()
      // conf.setAppName(s"CreateVertex()")
      // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // conf.setMaster("local[2]").set("spark.executor.memory", "1g")
      // val context = new SparkContext(conf)
      // val data = context.textFile(src)

      val job = new CreateGraph()
      job.graphOperations(src)

    } // end of else
  } // end of main

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

}


