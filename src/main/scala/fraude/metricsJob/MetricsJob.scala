package fraude.metricsJob

import fraude.sparkjob.SparkJob
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions.{col, lit, udf}
import fraude.confSpark.handler.HdfsHandler._

/**
  *
  * @param variableName          : Variable name from the attributes
  * @param min                   : Min metric
  * @param max                   : Max metric
  * @param mean                  : Mean metric
  * @param count                 : Count metric
  * @param missingValues         : Missing Values metric
  * @param variance              : Variance metric
  * @param standardDev           : Standard deviation metric
  * @param sum                   : Sum metric
  * @param skewness              : Skewness metric
  * @param kurtosis              : Kurtosis metric
  * @param percentile25          : Percentile25 metric
  * @param median                : Median metric
  * @param percentile75          : Percentile75 metric
  * @param category              : Category metric
  * @param countDistinct         : Count Distinct metric
  * @param countByCategory       : Count By Category metric
  * @param frequencies           : Frequency metric
  * @param missingValuesDiscrete : Missing Values Discrete metric
  */
case class MetricRow(
  variableName: String,
  min: Option[Double],
  max: Option[Double],
  mean: Option[Double],
  count: Option[Long],
  missingValues: Option[Long],
  variance: Option[Double],
  standardDev: Option[Double],
  sum: Option[Double],
  skewness: Option[Double],
  kurtosis: Option[Double],
  percentile25: Option[Double],
  median: Option[Double],
  percentile75: Option[Double],
  category: Option[List[String]],
  countDistinct: Option[Long],
  countByCategory: Option[Map[String, Long]],
  frequencies: Option[Map[String, Double]],
  missingValuesDiscrete: Option[Long],
)

object MetricsJob extends SparkJob {

  def run(): SparkSession = {
    sparkSession
  }




  /** Function that retrieves full metrics dataframe with both set discrete and continuous metrics
    *
    * @param dataMetric : dataframe obtain from computeDiscretMetric( ) or computeContinuiousMetric( )
    * @param listAttibutes : list of all variables
    * @param colName  : list of column
    * @return Dataframe : that contain the full metrics  with all variables and all metrics
    */

  def generateFullMetric(dataMetric : DataFrame, listAttibutes: List[String], colName: List[Column]): DataFrame= {
    listAttibutes.foldLeft(dataMetric){(data, nameCol) =>  data.withColumn(nameCol, lit(null) ) }.select(colName:_*)

  }

  /** Function Function that unifies discrete and continuous metrics dataframe, then write save the result to parquet
    *
    * @param discreteDataset : dataframe that contains all the discrete metrics
    * @param continuousDataset : dataframe that contains all the continuous metrics
    * @param savePathData  path where metrics are stored
    * @return
    */

  def unionDisContMetric( discreteDataset : DataFrame,
                          continuousDataset : DataFrame,
                          savePathData: Path) : DataFrame= {

  val listDiscAttrName: List[String] = List("min", "max", "mean", "count", "variance", "standardDev", "sum", "skewness", "kurtosis", "percentile25", "median", "percentile75", "missingValues")
  val listContAttrName: List[String] = List("category", "countDistinct","countByCategory", "frequencies", "missingValuesDiscrete")
  val listtotal: List[String] = List("variableName","min", "max", "mean", "count", "variance", "standardDev", "sum", "skewness", "kurtosis", "percentile25", "median", "percentile75", "missingValues","category", "countDistinct","countByCategory", "frequencies", "missingValuesDiscrete")
  val sortSelectCol : List[String] = List("variableName","min","max", "mean", "count", "missingValues", "standardDev", "variance", "sum", "skewness", "kurtosis", "percentile25", "median", "percentile75", "category", "countDistinct", "countByCategory", "frequencies", "missingValuesDiscrete")

  val neededColList: List[Column] = listtotal.map(x=> col(x) )


  val coupleDataMetrics = List((discreteDataset,listDiscAttrName),(continuousDataset,listContAttrName))

  val resultMetaDataFrame: DataFrame = coupleDataMetrics.map(tupleDataMetric => generateFullMetric(tupleDataMetric._1, tupleDataMetric._2, neededColList)).reduce(_ union _).select(sortSelectCol.head, sortSelectCol.tail: _*)

    if (exists(savePathData))
      delete(savePathData)


    val timeA000= System.nanoTime
    saveParquet(savePathData,resultMetaDataFrame)
    val durationA000= (System.nanoTime - timeA000) / 1e9d
    println("Time to save the result: " + durationA000)

    resultMetaDataFrame

  }


}
