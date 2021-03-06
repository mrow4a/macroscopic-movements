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
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.clustering.dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.rdd.RDD

/**
  * Area DBSCAN implementation which will bound points only for selected areas
  * and automatically decide about eps and minPoints based on area density
  * specified in the util/Config file
  *
  */
// TODO: In current version, BERLIN area is hardcoded !!!
object DBSCAN {

  /**
    * Train a DBSCAN Model using the given set of parameters
    *
    * @param data                  training points stored as `RDD[Vector]`
    *                              only the first two points of the vector are taken into consideration
    * @param eps                   the maximum distance between two points for them to be considered as part
    *                              of the same region - used mostly for efficient partitioning of data.
    * @param maxPointsPerPartition the largest number of points in a single partition
    */
  def train(
             data: RDD[Vector[String]],
             eps: Double,
             maxPointsPerPartition: Int): DBSCAN = {

    new DBSCAN(eps, maxPointsPerPartition, null, null).train(data)
  }
}

/**
  * A parallel implementation of DBSCAN clustering. The implementation will split the data space
  * into a number of partitions, making a best effort to keep the number of points in each
  * partition under `maxPointsPerPartition`. After partitioning, traditional DBSCAN
  * clustering will be run in parallel for each partition and finally the results
  * of each partition will be merged to identify global clusters.
  *
  * This is an iterative algorithm that will make multiple passes over the data,
  * any given RDDs should be cached by the user.
  */
class DBSCAN private(
                      val eps: Double,
                      val maxPointsPerPartition: Int,
                      @transient val partitions: List[(Int, DBSCANRectangle)],
                      @transient private val labeledPartitionedPoints: RDD[(Int, DBSCANLabeledPoint)]
                    )
  extends Serializable { // has to be Serializable

  type Margins = (DBSCANRectangle, DBSCANRectangle, DBSCANRectangle)
  type ClusterId = (Int, Int)

  def minimumRectangleSize = 2 * eps

  def labeledPoints: RDD[DBSCANLabeledPoint] = {
    labeledPartitionedPoints.values
  }

  /*
   * Main DBScan Logic
   */
  private def areaDBSCANNaive(stops: Iterable[DBSCANPoint])
  : Iterable[DBSCANLabeledPoint] = {
    val first = stops.head

    // ID of area to which point belonds
    var areaId = first.areaID

    // Eps of area to which point belonds -
    // the maximum distance between two points for them to be considered as part
    // of the same region
    var areaEps = first.areaEps

    // the minimum number of points required to form a dense region
    var areaMinPts = first.areaMinPts

    val clusteredPoints = new LocalDBSCANNaive(areaEps, areaMinPts)
      .fit(stops)

    clusteredPoints.map(point => adjustCluster(point, areaId))
  }

  def train(vectors: RDD[Vector[String]]): DBSCAN = {
    // generate the smallest rectangles that split the space
    // and count how many points are contained in each one of them
    val minimumRectanglesWithCount =
    vectors
      .map(toMinimumBoundingRectangle)
      .map((_, 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .collect()
      .toSet


    // find the best partitions for the data space
    val localPartitions = EvenSplitPartitioner
      .partition(minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize)

    // grow partitions to include eps
    val localMargins =
      localPartitions
        .map({ case (p, _) => (p.shrink(eps), p, p.shrink(-eps)) })
        .zipWithIndex

    val margins = vectors.context.broadcast(localMargins)

    // assign each point to its proper partition
    val duplicated = for {
      point <- vectors.map(DBSCANPoint)
      ((inner, main, outer), id) <- margins.value
      if outer.contains(point)
    } yield (id, point)

    val numOfPartitions = localPartitions.size
    // perform local dbscan
    val clustered =
      duplicated
        .groupByKey(numOfPartitions)
        .flatMapValues(clusterPoints)
        .cache()

    // find all candidate points for merging clusters and group them
    val mergePoints =
      clustered
        .flatMap({
          case (partition, point) =>
            margins.value
              .filter({
                case ((inner, main, _), _) => main.contains(point) && !inner.almostContains(point)
              })
              .map({
                case (_, newPartition) => (newPartition, (partition, point))
              })
        })
        .groupByKey()

    // find all clusters with aliases from merging candidates
    val adjacencies =
      mergePoints
        .flatMapValues(findAdjacencies)
        .values
        .collect()

    // generated adjacency graph
    val adjacencyGraph = adjacencies.foldLeft(DBSCANGraph[ClusterId]()) {
      case (graph, (from, to)) => graph.connect(from, to)
    }

    // find all cluster ids
    val localClusterIds =
      clustered
        .filter({ case (_, point) => point.flag != Flag.Noise })
        .mapValues(_.cluster)
        .distinct()
        .collect()
        .toList

    // assign a global Id to all clusters, where connected clusters get the same id
    val (total, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterId, Int]())) {
      case ((id, map), clusterId) => {

        map.get(clusterId) match {
          case None => {
            val nextId = id + 1
            val connectedClusters = adjacencyGraph.getConnected(clusterId) + clusterId
            val toadd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toadd)
          }
          case Some(x) =>
            (id, map)
        }

      }
    }

//    val line = eps + ", " + localClusterIds.size + "\n"
//    Writer.write(line)
//    logDebug("Wrote: " + line)

    val clusterIds = vectors.context.broadcast(clusterIdToGlobalId)

    // relabel non-duplicated points
    val labeledInner =
      clustered
        .filter(isInnerPoint(_, margins.value))
        .map {
          case (partition, point) => {

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            (partition, point)
          }
        }

    // de-duplicate and label merge points
    val labeledOuter =
      mergePoints.flatMapValues(partition => {
        partition.foldLeft(Map[DBSCANPoint, DBSCANLabeledPoint]())({
          case (all, (partition, point)) =>

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            all.get(point) match {
              case None => all + (point -> point)
              case Some(prev) => {
                // override previous entry unless new entry is noise
                if (point.flag != Flag.Noise) {
                  prev.flag = point.flag
                  prev.cluster = point.cluster
                }
                all
              }
            }

        }).values
      })

    val finalPartitions = localMargins.map {
      case ((_, p, _), index) => (index, p)
    }

    new DBSCAN(
      eps,
      maxPointsPerPartition,
      finalPartitions,
      labeledInner.union(labeledOuter)
    )

  }

  private def clusterPoints(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {
    // Assign points to proper area
    points
      .groupBy(_.areaID).values
      // Cluster points
      .flatMap(areaDBSCANNaive)
  }

  private def adjustCluster(point: DBSCANLabeledPoint, areaID : Int): DBSCANLabeledPoint = {
    val newClusterId = areaID + "" + point.cluster
    if (point.cluster != 0)
      point.cluster = newClusterId.toInt

    point
  }


  /**
    * Find the appropriate label to the given `vector`
    *
    * This method is not yet implemented
    */
  def predict(vector: Vector[String]): DBSCANLabeledPoint = {
    throw new NotImplementedError
  }

  private def isInnerPoint(
                            entry: (Int, DBSCANLabeledPoint),
                            margins: List[(Margins, Int)]): Boolean = {
    entry match {
      case (partition, point) =>
        val ((inner, _, _), _) = margins.filter({
          case (_, id) => id == partition
        }).head

        inner.almostContains(point)
    }
  }

  private def findAdjacencies(
                               partition: Iterable[(Int, DBSCANLabeledPoint)]): Set[((Int, Int), (Int, Int))] = {

    val zero = (Map[DBSCANPoint, ClusterId](), Set[(ClusterId, ClusterId)]())

    val (seen, adjacencies) = partition.foldLeft(zero)({

      case ((seen, adjacencies), (partition, point)) =>

        // noise points are not relevant for adjacencies
        if (point.flag == Flag.Noise) {
          (seen, adjacencies)
        } else {

          val clusterId = (partition, point.cluster)

          seen.get(point) match {
            case None => (seen + (point -> clusterId), adjacencies)
            case Some(prevClusterId) => (seen, adjacencies + ((prevClusterId, clusterId)))
          }

        }
    })

    adjacencies
  }

  private def toMinimumBoundingRectangle(vector: Vector[String]): DBSCANRectangle = {
    val point = DBSCANPoint(vector)
    val x = corner(point.x)
    val y = corner(point.y)
    DBSCANRectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
  }

  private def corner(p: Double): Double =
    (shiftIfNegative(p) / minimumRectangleSize).intValue * minimumRectangleSize

  private def shiftIfNegative(p: Double): Double =
    if (p < 0) p - minimumRectangleSize else p

}
