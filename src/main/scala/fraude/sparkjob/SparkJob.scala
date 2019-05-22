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

package fraude.sparkjob

import metric.analysis.conf.HdfsConf._
import metric.analysis.handler.HdfsHandler._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column,DataFrame, Dataset, Row, SparkSession}
import org.apache.log4j.{Level, Logger}


trait  SparkJob {

  val conf: SparkConf = new SparkConf()
    .setAppName("Spark training exercises")
    .setMaster("local[*]")
    .set("fs.defaultFS", load().get("fs.defaultFS"))

    // set the spark-hitory-server
    .set("spark.eventLog.enabled","true")
    .set("spark.eventLog.dir","hdfs://localhost:9000/spark/applicationHistory")
    .set("spark.history.fs.logDirectory","hdfs://localhost:9000/spark/applicationHistory")

  val sparkSession: SparkSession =
    SparkSession.builder().config(conf).getOrCreate()


  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)


  /**
    * Computes the the runtime of a code
    * @param block
    * @tparam R
    * @return
    */

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }


  /**
    * Reads the csv files specified in the path into Dataframe
    * @param path path to start reading files from
    * @return DataFrame corresponding to the read files.
    */
  def read(path: Path): DataFrame = {
    sparkSession.read.option("header", "true").csv(path.toString)
  }

  /**
    * Reads the CSV files specified in the path into RDDs
    * @param path path to start reading files from
    * @return RDD of Row corresponding to the read files.
    */
  def readRDD(path: Path): RDD[Row] = {
    sparkSession.read.option("header","true").csv(path.toString).rdd
  }

  /**
    * Saves a DataSet of generic type T into a parquet file
    * @param path path to save the dataframe in
    * @param df   DataSet to save
    * @return boolean corresponds to whether or not the path exists
    */
  def saveParquet[T](path: Path, ds: Dataset[T]): Boolean = {
    ds.coalesce(1).write.parquet(path.toString)
    exists(path)
  }

 // def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))
 // val discreteDatasetTafeng2 =discreteDatasetTafeng.withColumn("category", stringify(discreteDatasetTafeng("category")))


  def saveCSV[T](path: Path, ds: Dataset[T]): Boolean = {
  ds.coalesce(1)
    .write
    .option("header", "true")
    .option("sep", '\t')
    .mode("append")
    .csv(path.toString)
    exists(path)
  }
}







