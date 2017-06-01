package util

import movements.StopDetectionJob

/**
  * Created by gabri on 2017-06-01.
  */
object DBSCANrunner {

  def main(args: Array[String]) {
    // resources\Locker\macroscopic-movement-01_areafilter.csv 100000 0.001 5
    val file = "resources\\Locker\\macroscopic-movement-01_areafilter.csv"
    val partitionSize = "100000"
    val radius = "0.0002" // 0.001 = 1 km, min distance between 2 points = 0.1112 km
    val minPts = "5"
    val array = Array(file, partitionSize, radius, minPts)

    for(i <- 0 to 10) {
      StopDetectionJob.main(array)

      array(3) = (array(3).toDouble + 0.0001).toString
    }
  }


}
