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

package fraude

import com.typesafe.scalalogging.StrictLogging
import fraude.confSpark.conf.Settings
import fraude.sparkjob.SparkJob
import fraude.metricsJob.MetricsJob._
import fraude.metricsJob.Metrics
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame


object Main extends SparkJob with StrictLogging{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block    // call-by-name
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0) + "ns")
      result
    }


    println("----------------------------")
    println("   Load the dataset.csv    :")
    println("----------------------------")


    val filename: String = "creditcard"
    val inputPathData: Path = new Path(Settings.sparktrain.inputPath ++ "fraud_detection" +"/"+ filename ++".csv")

    val inputDataFrame: DataFrame =  read(inputPathData)
    inputDataFrame.show(20)


    println("----------------------------")
    println("  Test With Titanic data set:")
    println("----------------------------")
    val totalDiscreteMetrics: List[Metrics.DiscreteMetric] = List(Metrics.Category, Metrics.CountDistinct, Metrics.CountDiscrete, Metrics.Frequencies,Metrics.CountMissValuesDiscrete)
    val discreteOperation: List[Metrics.DiscreteMetric] = totalDiscreteMetrics


    val discreteOps: List[Metrics.DiscreteMetric] = totalDiscreteMetrics
    val continuousOps:  List[Metrics.ContinuousMetric] = totalContMetric


    val savePathData: Path = new Path(Settings.sparktrain.savePath ++ "testMetricTitanic/")


    val dataUse: DataFrame = inputDataFrame2

    val discAttrs: List[String] = titanicDiscreteAttributes
    val continAttrs: List[String] = titanicContinuousAttributes

    val timeA01= System.nanoTime
    val discreteDatasetTitanic = Metrics.computeDiscretMetric(dataUse, discAttrs, discreteOps, 1000)
    val continuousDatasetTitanic =  Metrics.computeContinuiousMetric(dataUse, continAttrs, continuousOps)
    val durationA01= (System.nanoTime - timeA01) / 1e9d
    println("Time to compute  all the metrics: " + durationA01)

    val timeA02= System.nanoTime
    val resultatSaveTitanic : DataFrame = unionDisContMetric( discreteDatasetTitanic,continuousDatasetTitanic,
      "domain",
      "schema",
      "ingestionTime",
      "stageState",savePathData: Path)
    val durationA02= (System.nanoTime - timeA02) / 1e9d
    println("Time to make the union and to save : " + durationA02)
    resultatSaveTitanic.show()


  }

}


