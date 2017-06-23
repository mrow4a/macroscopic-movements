package util

import java.io.{BufferedWriter, File, FileWriter}

/**
  * Created by gabri on 2017-06-22.
  */
object Writer {

  val dstFile = new File("resources/Locker/dbscan/eps_histogram_3")
  val bufferedWriter = new BufferedWriter(new FileWriter(dstFile))

  def write(line: String): Unit = {
    bufferedWriter.write(line)
    // logDebug("Wrote: " + line)
  }

  def close() = bufferedWriter.close()
}
