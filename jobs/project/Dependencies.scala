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
import sbt._

object Dependencies {

  import Versions._

  // NettyIo is very bad one, the organization name is different
  // from the jar name for older versions
  val excludeNettyIo = ExclusionRule(organization = "org.jboss.netty")

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % spark % "provided" excludeAll (excludeNettyIo),
    // Force netty version.  This avoids some Spark netty dependency problem.
    "com.meetup" %% "archery" % "0.4.0",
    "io.netty" % "netty-all" % netty
  )

  lazy val sparkExtraDeps = Seq(
    "org.apache.spark" %% "spark-sql" % spark % "provided" excludeAll (excludeNettyIo),
    "org.apache.spark" %% "spark-streaming" % spark % "provided" excludeAll (excludeNettyIo)
  )

//  lazy val jobserverDeps = Seq(
//    "spark.jobserver" % "job-server-api_2.11" % jobServer % "provided",
//    "spark.jobserver" % "job-server-extras_2.11" % jobServer % "provided"
//  )

  // This is needed or else some dependency will resolve to 1.3.1 which is in jdk-8
  lazy val typeSafeConfigDeps = Seq("com.typesafe" % "config" % typesafeConfig force())

  lazy val coreTestDeps = Seq()

  val repos = Seq(
//    "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
    "bintray/meetup" at "http://dl.bintray.com/meetup/maven"
  )
}
