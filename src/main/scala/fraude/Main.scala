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


    val filename: String = "iris"
    val inputPathData: Path = new Path(Settings.sparktrain.inputPath ++ filename +"/"+ filename ++".csv")

    val listContnuousAttributes: List[String] = Seq("SepalLength", "SepalWidth","PetalLength","PetalWidth").toList
    val listDiscreteAttributes: List[String] = Seq("Name").toList

    val inputDataFrame1: DataFrame = {
      read(inputPathData)
    }


    val filenametitanic: String = "titanic"
    val inputPathData2: Path = new Path(Settings.sparktrain.inputPath ++ filenametitanic +"/"+ filenametitanic ++".csv")

    val inputDataFrame2: DataFrame = read(inputPathData2)

    val titanicContinuousAttributes: List[String] = Seq("Fare", "Age").toList
    val titanicDiscreteAttributes: List[String] = Seq("Survived", "Pclass","Siblings","Parents","Sex").toList


    val filenametafeng: String = "tafeng"
    val inputPathData3: Path = new Path(Settings.sparktrain.inputPath ++ filenametafeng +"/"+ filenametafeng ++".csv")

    val inputDataFrame3: DataFrame =read(inputPathData3)

    val tafengContinuousAttributes: List[String] =  Seq("AMOUNT","ASSET","SALES_PRICE").toList
    val tafengDiscreteAttributes: List[String] = Seq("TRANSACTION_DT","CUSTOMER_ID","AGE_GROUP","PIN_CODE","PRODUCT_SUBCLASS","PRODUCT_ID").toList





  }

}


