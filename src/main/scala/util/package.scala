/**
  * Created by gabri on 2017-05-24.
  */
package object util {

  implicit class StringImprovements(val s: String) {
    import scala.util.control.Exception._
    def toIntOpt = catching(classOf[NumberFormatException]) opt s.toInt
  }
}
