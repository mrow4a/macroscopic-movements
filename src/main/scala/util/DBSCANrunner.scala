package util

import movements.{ClusterStopsJob}

/**
  * Created by gabri on 2017-06-01.
  */
object DBSCANrunner {

  def main(args: Array[String]) {
    // resources\Locker\macroscopic-movement-01_areafilter.csv 100000 0.001 5
    val file = "resources\\Locker\\macroscopic-movement-01_areafilter.csv"
    val partitionSize = 100000
    var radius = 0.001
    //radius = 0.001
    // 0.01 = 1.1 km, min distance between 2 points = 0.1112 km
    var minPts = 1
    val limit = 20
    for (i <- 0 to limit) {

      val array = Array(file, partitionSize.toString, radius.toString, minPts.toString)

      minPts += 1
      ClusterStopsJob.main(array)


    }
  }


}
