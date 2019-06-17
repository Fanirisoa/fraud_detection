/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

import sbt._

object Dependencies {

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest,
    "org.scalatest" %% "scalatest" % Versions.scalatest % "test"
  )

  val logging = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
  )

  val typedConfigs = Seq("com.github.kxbmap" %% "configs" % Versions.configs)
  
  
  val spark = Seq(
    "org.apache.spark" %% "spark-core"  % Versions.spark, // %"provided",
    "org.apache.spark" %% "spark-sql"   % Versions.spark, // %"provided",
    "org.apache.spark" %% "spark-hive"  % Versions.spark,// %"provided",
    "org.apache.spark" %% "spark-mllib" % Versions.spark
  )

  val xGBoost = Seq(
    "ml.dmlc" % "xgboost4j-spark" % Versions.xGBoost,
    "ml.dmlc" % "xgboost4j" % Versions.xGBoost
  )

  val dependencies = spark ++ scalaTest ++ logging ++ typedConfigs++xGBoost

}


