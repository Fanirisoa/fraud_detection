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
import fraude.metricsJob.MetricsJob.{sparkSession, _}
import fraude.metricsJob.Metrics
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


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

    /*
    println("----------------------------")
    println("   Load the dataset.csv    :")
    println("----------------------------")


    val filename: String = "creditcard"
    val inputPathData: Path = new Path(Settings.sparktrain.inputPath ++ "fraud_detection" +"/"+ filename ++".csv")

    val inputDataFrame: DataFrame =  read(inputPathData)
    inputDataFrame.show(20)

    val listContnuousAttributes: List[String] =     Seq("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount").toList
    val listDiscreteAttributes: List[String] =     Seq("Class").toList


    println("--------------------")
    println("  Compute Metrics  :")
    println("--------------------")
    val totalDiscreteMetrics: List[Metrics.DiscreteMetric] = List(Metrics.Category, Metrics.CountDistinct, Metrics.CountDiscrete, Metrics.Frequencies,Metrics.CountMissValuesDiscrete)
    val totalContMetric : List[Metrics.ContinuousMetric]=  List(Metrics.Min, Metrics.Max, Metrics.Mean, Metrics.Count, Metrics.Variance, Metrics.Stddev, Metrics.Sum, Metrics.Skewness, Metrics.Kurtosis, Metrics.Percentile25, Metrics.Median, Metrics.Percentile75, Metrics.CountMissValues)


    val discreteOps: List[Metrics.DiscreteMetric] = totalDiscreteMetrics
    val continuousOps:  List[Metrics.ContinuousMetric] = totalContMetric


    val savePathData: Path = new Path(Settings.sparktrain.savePath ++ "MetricResult/")

    val dataUse: DataFrame = inputDataFrame

    val continAttrs: List[String] = listContnuousAttributes
    val discAttrs: List[String] = listDiscreteAttributes


    val timeA01= System.nanoTime
    val discreteDataset = Metrics.computeDiscretMetric(dataUse, discAttrs, discreteOps, 1000)
    val continuousDataset=  Metrics.computeContinuiousMetric(dataUse, continAttrs, continuousOps)
    val durationA01= (System.nanoTime - timeA01) / 1e9d
    println("Time to compute  all the metrics: " + durationA01)

    discreteDataset.show(20)
    continuousDataset.show(20)


    val timeA02= System.nanoTime
    val resultatSaveTitanic : DataFrame = unionDisContMetric( discreteDataset,continuousDataset,savePathData: Path)
    val durationA02= (System.nanoTime - timeA02) / 1e9d
    println("Time to make the union and to save : " + durationA02)
    resultatSaveTitanic.show()


*/


    def run(): SparkSession = {
      sparkSession
    }

    import sparkSession.sqlContext.implicits._


   /*
    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500,250,100)),
      (2, "Michael Armbrust", 1, Seq(250,100))).toDF("id", "name","graduate_program","spark_status")

    person.show()

    val graduateProgram = Seq(
      (0, "Masters", "School of information", "UC Berkeley"),
      (1, "Masters", "EECS", "UC Berkeley"),
      (2, "PHD", "EECS", "UC Berkeley")).toDF("id", "degree","departement","school")

    graduateProgram.show()
    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor")).toDF("id", "status")
    sparkStatus.show()

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    val joinExpression: Column = person.col("graduate_program") === graduateProgram.col("id")


    person.join(graduateProgram,joinExpression).show()


    person.join(graduateProgram,joinExpression,"outer").show()

    graduateProgram.join(person, joinExpression, "left_outer").show()

    person.join(graduateProgram, joinExpression, "right_outer").show()

    graduateProgram.join(person, joinExpression, "left_semi").show()

    graduateProgram.join(person, joinExpression, "left_anti").show()

    graduateProgram.join(person, joinExpression, "cross").show()

    person.crossJoin(graduateProgram).show()

    */


    /*
    val mycollection: Array[String] = "Spark the definitive Guide : BIg Data processing Made Simple"
      .split(" ")

    val words : RDD[String] = sparkSession.sparkContext.parallelize(mycollection,2)

    words.toDF.show()

    words.map(word => (word.toLowerCase, 1))

     val keyword: RDD[(String, String)] = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

    keyword.toDF.show()


    println(keyword.lookup("s"))

    val chars: RDD[Char] = words.flatMap(word => word.toLowerCase.toSeq)
    val KVvharacters = chars.map(letter => (letter, 1))
    def maxFunc(left : Int, rigth : Int): Int = math.max(left, rigth)
    def addFunc(left : Int, rigth : Int): Int = left + rigth
    val nums: RDD[Int] = sparkSession.sparkContext.parallelize(1 to 30, 5)

    println(KVvharacters.countByKey())


    println(" the result: " + nums.aggregate(0)(maxFunc,addFunc))


    val numRange = sparkSession.sparkContext.parallelize(0 to 9, 2)
    words.zip(numRange).collect()


    */


    val myCollection: Array[String] = " Spark The definitve Guide : Big Data Processing Mase Simple".split(" ")
    println(myCollection)
    val words = sparkSession.sparkContext.parallelize(myCollection,2)


    val supplementalData = Map("Spark" -> 1000,"Definitive" -> 200, "Big" -> -300, "Simple" -> 100)
    val suppBroadcast: Broadcast[Map[String, Int]] = sparkSession.sparkContext.broadcast( supplementalData )

    suppBroadcast.value







  }

}


