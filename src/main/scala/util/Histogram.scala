package util

/**
  * Created by gabri on 2017-06-07.
  */

import org.sameersingh.scalaplot.Implicits._

object Histogram {

  def main(args: Array[String]) {
    /*   val events = cmsdata.EventIterator()

       val histogram = Bin(10, 0, 100, { event: Event => event.met.pt })

       for (event <- events.take(1000))
         histogram.fill(event)

       println(histogram)
       // histogram.println
   */


    val x = 0.0 until 2.0 * math.Pi by 0.1
    output(ASCII, xyChart(x ->(math.sin(_), math.cos(_))))
  }

}
