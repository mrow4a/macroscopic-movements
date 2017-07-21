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

package movements.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ValidateFileJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()

    // NOTE: Without below lines, if spark cluster consists only of master node,
    // it will not run:
    //
    // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // conf.setMaster("local[*]").set("spark.executor.memory", "1g")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    if (args.length < 2) {
      throw new Exception("No s3 endpoint or s3a:// path given. "
        + "Example: http://localhost:9000 s3a://movements:movements@movements/macroscopic-movement-01_areafilter.csv")
    }
    var endpoint = args(0) // need to pass endpoint as arg
    var src = args(1) // need to pass file as arg
    var dst = args(2) // need to pass dst as arg

    sc.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)

    val data = sc.textFile(src)

    val parsedData = data.map(s => s.split(';').toVector)
    println(parsedData.first())

    // TODO: Validate file

    sc.stop()
  }

}