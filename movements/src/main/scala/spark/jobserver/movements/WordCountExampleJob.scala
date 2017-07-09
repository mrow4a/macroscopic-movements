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
package spark.jobserver.movements

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import spark.jobserver.api.{SparkJob => NewSparkJob}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try

/**
 * A super-simple Spark job example that implements the SparkJob trait and can be submitted to the job server.
 *
 * Set the config with the sentence to split or count:
 * input.string = "adsfasdf asdkf  safksf a sdfa"
 *
 * validate() returns SparkJobInvalid if there is no input.string
 */
object WordCountExampleJob extends SparkJob {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("input.path = a b s d aaa a a")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.path"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.path config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    sc.parallelize(config.getString("input.path").split(" ").toSeq).countByValue
  }
}

