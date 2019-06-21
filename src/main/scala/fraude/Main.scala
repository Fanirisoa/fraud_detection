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
import fraude.XGBoostJOB.XGBoost.ParamXGBoostClassifier
import fraude.confSpark.conf.Settings
import fraude.sparkjob.SparkJob
import fraude.XGBoostJOB.XGBoost._
import fraude.XGBoostJOB.XGBoostCVTune._
import fraude.metricsJob.MetricsJob.{sparkSession, _}
import fraude.metricsJob.BasicStatistics
import fraude.smoteOverSample.smoteClass._
import fraude.smoteOverSample.KnnJob._
import fraude.metricsJob.Correlation
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._



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
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             val allAttributesList :List[String] =     Seq("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount","Class").toList
    val listContnuousAttributes: List[String] =     Seq("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount").toList
    val listDiscreteAttributes: List[String] =     Seq("Class").toList
    //  val allAttributesList :List[String] =     Seq("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9","V10","V11",  "Amount","Class").toList

    val taux: Double = (2000.00 * 1.00) / 284315.00
    println(taux)

    val ids = Seq(0)
    val dataZero = inputDataFrame.filter(col("Class") === 0).sample(taux)
    val dataOne = inputDataFrame.filter(col("Class") === 1)

    val dataUse = dataZero.union(dataOne)


/*

        println("----------------------------")
        println("  Compute BasicStatistics  :")
        println("----------------------------")
        val totalDiscreteMetrics: List[BasicStatistics.DiscreteMetric] = List(BasicStatistics.Category, BasicStatistics.CountDistinct, BasicStatistics.CountDiscrete, BasicStatistics.Frequencies,BasicStatistics.CountMissValuesDiscrete)
        val totalContMetric : List[BasicStatistics.ContinuousMetric]=  List(BasicStatistics.Min, BasicStatistics.Max, BasicStatistics.Mean, BasicStatistics.Count, BasicStatistics.Variance, BasicStatistics.Stddev, BasicStatistics.Sum, BasicStatistics.Skewness, BasicStatistics.Kurtosis, BasicStatistics.Percentile25, BasicStatistics.Median, BasicStatistics.Percentile75, BasicStatistics.CountMissValues)



        val discreteOps: List[BasicStatistics.DiscreteMetric] = totalDiscreteMetrics
        val continuousOps:  List[BasicStatistics.ContinuousMetric] = totalContMetric


        val savePathData: Path = new Path(Settings.sparktrain.savePath ++ "MetricResult/")



        val continAttrs: List[String] = listContnuousAttributes
        val discAttrs: List[String] = listDiscreteAttributes


        val timeA01= System.nanoTime
        val discreteDataset = BasicStatistics.computeDiscretMetric(dataUse, discAttrs, discreteOps, 1000)
        val continuousDataset=  BasicStatistics.computeContinuiousMetric(dataUse, continAttrs, continuousOps)
        val durationA01= (System.nanoTime - timeA01) / 1e9d
        println("Time to compute  all the metrics: " + durationA01)


        val timeA02= System.nanoTime
        val resultatSave : DataFrame = unionDisContMetric(discreteDataset,continuousDataset,savePathData: Path)
        val durationA02= (System.nanoTime - timeA02) / 1e9d
        println("Time to make the union and to save : " + durationA02)
        resultatSave.show()


        println("-------------------------------")
        println("  Compute Correlation Matrix  :")
        println("-------------------------------")

        val timeA03= System.nanoTime
        val correlationMatrix: DataFrame = Correlation.computeCorrelationMatrix(dataUse, allAttributesList)
        val durationA03= (System.nanoTime - timeA03) / 5e9d
        println("Time to compute the correlation matrix: " + durationA03)
        correlationMatrix.show(50)

*/


    println("-------------------------------")
    println("    Compute KNNcalculation    :")
    println("-------------------------------")

    val dataReduice: DataFrame = dataUse

    val allAssembly :List[String] =     Seq("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount").toList
    val dataAssembly: DataFrame =  featureAssembler(dataReduice,allAssembly,"Class")
    dataAssembly.show()


    val timeA04= System.nanoTime
    val dataKNN: DataFrame =  KNNCalculation(dataAssembly,
      "feature",
      "Class",
      3,
      2,
      3)
    val durationA04= (System.nanoTime - timeA04) / 5e9d
    println("Time to compute The KNN : " + durationA04)


    // percentOver = 100 ===> Bucketlength = 2 , percentOver = 200 ===> Bucketlength = 5
    val timeA05= System.nanoTime
    val resSmote: DataFrame = Smote( dataReduice, allAssembly, "Class", 200, 5, 3)
    val durationA05= (System.nanoTime - timeA05) / 5e9d
    println("Time to compute the SmoteOversampling: " + durationA05)
    resSmote.show()


    val dataDiAssembly: DataFrame =  featureDiAssembler(resSmote,allAssembly)
    dataDiAssembly.show()




    println("-------------------------------")
    println("        Using XGBOOST         :")
    println("-------------------------------")

    val splitLevel : Double = 0.8
    val listColFeatures : Array[String] = allAssembly.toArray
    val nameColClass : String = "Class"
    val inputDataFram: DataFrame = dataDiAssembly
    val paramClassifier : ParamXGBoostClassifier = ParamXGBoostClassifier(0.3f, 2,  "multi:softprob", 2,200, 3)


    val timeA06= System.nanoTime
    val resultPrediction: DataFrame =  xGBoostSimplePrediction(
      splitLevel,
      listColFeatures,
      nameColClass ,
      inputDataFram,
      paramClassifier
    )
    val durationA06= (System.nanoTime - timeA06) / 5e9d
    println("Time to compute the SmoteOversampling: " + durationA06)
    resSmote.show()
    resultPrediction.show(false)

    val resultEvaluation: Double=  evalPrediction(resultPrediction)
    println(resultEvaluation)



    val splitLevel2 : Double = 0.82
    val foldNum = 10

    val timeA07= System.nanoTime
    val resultPrediction2 =  xGBoostcrossValTune(
      splitLevel2,
      listColFeatures,
      nameColClass,
      inputDataFram,
      foldNum
    )
    val durationA07= (System.nanoTime - timeA07) / 5e9d
    println("Time to compute the SmoteOversampling: " + durationA07)
    resSmote.show()
    resultPrediction2.show(false)


    val resultEvaluation2: Double=  evalPrediction(resultPrediction2)
    println(resultEvaluation)
    println(resultEvaluation2)











/*
    val fileiris: String = "iris"
    val irisPathData: Path = new Path(Settings.sparktrain.inputPath ++ "/"+ "iris" +"/"+ fileiris ++".csv")



    val irisDataFrame: DataFrame =  read(irisPathData)
    irisDataFrame.show()
    irisDataFrame.printSchema()



    val splitLevel : Double = 0.8
    val listColFeatures : Array[String] = Array("SepalLength", "SepalWidth", "PetalLength", "PetalWidth")
    val nameColClass : String = "Name"
    val inputDataFram: DataFrame = irisDataFrame
    val paramClassifier : ParamXGBoostClassifier = ParamXGBoostClassifier(0.3f, 2,  "multi:softprob", 3,200, 3)



    val resultPrediction: DataFrame =  xGBoostSimplePrediction(
                                                                splitLevel,
                                                                listColFeatures,
                                                                nameColClass ,
                                                                inputDataFram,
                                                                paramClassifier
                                                              )
    resultPrediction.show(false)


    val splitLevel2 : Double = 0.82
    val foldNum = 4

    val resultPrediction2 =  xGBoostcrossValTune(
                                                  splitLevel2,
                                                  listColFeatures,
                                                  nameColClass,
                                                  inputDataFram,
                                                  foldNum
                                                )
    resultPrediction2.show(false)

    val resultEvaluation: Double=  evalPrediction(resultPrediction)
    val resultEvaluation2: Double=  evalPrediction(resultPrediction2)
    println(resultEvaluation)
    println(resultEvaluation2)



 */
  }
}


