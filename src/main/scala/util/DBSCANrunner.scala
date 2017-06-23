package util

import movements.ClusterStopsJob
import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import stopdetection.StopDetection

/**
  * Created by gabri on 2017-06-01.
  */
object DBSCANrunner {

  val log = LoggerFactory.getLogger(ClusterStopsJob.getClass)

  def main(args: Array[String]) {
    log.info("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"DBSCAN histogram")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    log.info("Parse Input File to StopPoint class instances")
    val src = "resources\\Locker\\macroscopic-movement-01_areafilter.csv"
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

   // val dstFile = new File("resources/Locker/dbscan/histogram")
  //  val bufferedWriter = new BufferedWriter(new FileWriter(dstFile))

    var maxPointsPerPartition = 10000
    var eps = 0.0029
    var minPoints = 5

    log.debug("Cluster Points")
    val runs = 11
    for (i <- 0 to runs) {
      val dbScanModel = DBSCAN.train(
        detectedStops,
        eps,
        minPoints,
        maxPointsPerPartition)

      eps += 0.0001
      // minPoints += 1

    }
    Writer.close()
  }

  /*
      def main(args: Array[String]) {

      val dstFile = new File("resources/Locker/dbscan/histogram")
      val bw = new BufferedWriter(new FileWriter(dstFile))

      // resources\Locker\macroscopic-movement-01_areafilter.csv 100000 0.001 5
      val srcFile = "resources\\Locker\\macroscopic-movement-01_areafilter.csv"
      val partitionSize = 100000
      var radius = 0.001
      //radius = 0.001
      // 0.01 = 1.1 km, min distance between 2 points = 0.1112 km
      var minPts = 5
      val limit = 10
      for (i <- 0 to limit) {

        val array = Array(srcFile, partitionSize.toString, radius.toString, minPts.toString)

        minPts += 1
        val c = ClusterStopsJob.main(array)
        bw.write(text)

      }
      bw.close()
    }
    */


}
