package util

import movements.StopDetectionJob

/**
  * Created by gabri on 2017-06-01.
  */
object DBSCANrunner {

  def main(args: Array[String]) {
    // resources\Locker\macroscopic-movement-01_areafilter.csv 100000 0.001 5
    val file = "C:\\Users\\gabri\\Documents\\Workspace\\TU-Berlin\\ISOL6\\MacroMovements\\resources\\Locker\\macroscopic-movement-01_areafilter.csv"
    val partitionSize = 100000
    var radius = 0.001
    //radius = 0.001
    // 0.01 = 1.1 km, min distance between 2 points = 0.1112 km
    var minPts = 5
    val limit = 10
    for (i <- 0 to limit) {

      val array = Array(file, partitionSize.toString, radius.toString, minPts.toString)

      radius += -0.00001
      StopDetectionJob.main(array)


    }
  }


}
