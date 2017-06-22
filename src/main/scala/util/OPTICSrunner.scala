package util

import com.hansight.algorithms.mllib.ParallelOptics
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by gabri on 2017-06-20.
  */
object OPTICSrunner {

  def main(args: Array[String]): Unit = {

    // step 1: creat a Spark context
    val conf = new SparkConf().setMaster("local[*]").setAppName("POptics").set("spark.default.parallelism", "4")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    // setp 2: run ParalleOptics
    val minPts = 5
    val radius = 10000
    val src = "resources\\Locker\\macroscopic-movement-01_areafilter.csv"
    val data = sc.textFile(src).map { (line: String) =>
      //do stuff with line like
      line.concat("/")
    }

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    /*val data = Source
      .fromFile("resources\\Locker\\macroscopic-movement-01_areafilter.csv")
      .getLines
      .map { (line: String) =>
        //do stuff with line like
        line.concat("/")
      }*/
    val opt = new ParallelOptics(minPts, radius)
    val out = opt.run(data) // TODO: exception in thread "main" scala.MatchError: (MapPartitionsRDD[2] at map at OPTICSrunner.scala:24,0;04:27:13;0a27d60f42f88a40560270ba7291e4b3438c1a832f43aaf66144f0d8034d1e50; 52.55; 13.403/,EuclideanDistance)

  }

}
