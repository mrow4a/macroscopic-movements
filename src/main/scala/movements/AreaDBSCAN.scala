package movements

import java.util.Random

import org.apache.spark.mllib.clustering.dbscan.{DBSCAN, DBSCANPoint, DBSCANRectangle}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import stopdetection.StopDetection

/**
  * Runs area version of DBSCAN.
  *
  * Created by gabri on 2017-06-01.
  *
  */
object AreaDBSCAN {

  val log = LoggerFactory.getLogger(ClusterStopsJob.getClass)


  def main(args: Array[String]) {
    log.info("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"DBSCAN")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    log.info("Parse Input File to StopPoint class instances")
    val src = args(0)
    val data = sc.textFile(src)

    val parsedData = data.map(s => s.split(';').toVector)

    log.info("Filter Moves to obtain stops only")

    val durationsSlidingWindowSize = 2400.0
    // By default 40 minutes
    val stopCertaintyMaxDistance = 1500.0
    // By default max walking distance for human
    val stopCertaintyMaxSpeed = 0.833
    // By default min human walking speed
    val travelCertaintyMinSpeed = 1.4 // By default max human walking speed

    val detectedStops = StopDetection.filter(
      parsedData,
      durationsSlidingWindowSize,
      stopCertaintyMaxDistance,
      stopCertaintyMaxSpeed,
      travelCertaintyMinSpeed)

    log.debug("Cluster Points")

    val innerArea: DBSCANRectangle = DBSCANRectangle(52.4425, 13.2582, 52.5647, 13.4818)
    val middleArea: DBSCANRectangle = DBSCANRectangle(52.3446, 13.0168, 52.6375, 13.6603)

    val innerStops = detectedStops.filter(p => innerArea.contains(DBSCANPoint(p)))
    // inner
    val outerStops = detectedStops.filter(p => !middleArea.contains(DBSCANPoint(p)))
    // outer
    val middleStops = detectedStops.subtract(outerStops).subtract(innerStops) // middle = detected - outer - inner

    log.debug("Cluster Points")
    var maxPointsPerPartition = 10000
    var eps = 0.001
    var minPoints = 5
    var areaId = 1

    val clusteredData = runDBSCAN(innerStops, 0.001, minPoints, maxPointsPerPartition, 1) ++
      runDBSCAN(middleStops, 0.003, minPoints, maxPointsPerPartition, 2) ++
      runDBSCAN(outerStops, 0.005, minPoints, maxPointsPerPartition, 3)

    writeToFile(clusteredData, eps, minPoints)

    log.info("Stopping Spark Context...")
    sc.stop()
  }


  private def runDBSCAN(innerStops: RDD[Vector[String]], eps: Double, minPoints: Int, maxPointsPerPartition: Int, areaID: Int)
  : RDD[String] = {
    DBSCAN.train(innerStops, eps, minPoints, maxPointsPerPartition)
      .labeledPoints.map(p => s"${p.id},${p.x},${p.y},${
      if (p.cluster == 0) 0 else areaID + "" + p.cluster
    }")
  }

  private def writeToFile(clusteredData: RDD[String], eps: Double, minPoints: Int) = {
    log.debug("Save points to the result file")

    val random = new Random()
    //val clusteredData = dbScanModel.labeledPoints.map(p => s"${p.id},${p.x},${p.y},${p.cluster}")
    val filePath = "resources/Locker/dbscan/v2/" + eps + "_" + minPoints + "_" + random.nextInt()
    clusteredData.coalesce(1).saveAsTextFile(filePath)

    log.info("Wrote result to " + filePath)
  }

}
