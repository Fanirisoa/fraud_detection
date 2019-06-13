package fraude.metricsJob

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


object Correlation extends StrictLogging {

  def regroupCorrelationByVariable(nameCol: String, metricFrame: DataFrame ): DataFrame = {
    //Get the whole list of headers that contains the column name
    val listColumns: Array[String] = metricFrame.columns.filter(_.contains("corr("+nameCol+",")).sorted
    //Select only columns in listColumns
    val selectedListColumns: DataFrame = metricFrame.select(listColumns.head, listColumns.tail: _*)
    //Reduce decimal values to 3
    val broundColumns: DataFrame = selectedListColumns.select(
      selectedListColumns.columns.map(c => bround(col(c), 3).alias(c)): _*
    )
    //Add column Variables that contains the nameCol
    val addVariablesColumn: DataFrame = broundColumns.withColumn("variableName", lit(nameCol))


    //Remove nameCol to keep only the metric name foreach metric.
    val removeNameColumnMetric = addVariablesColumn.columns.toList
      .map(str => str.replaceAll("corr"+"\\(" + nameCol + "\\, ", ""))
      .map(str => str.replaceAll( "\\)", ""))
      .map(_.capitalize)

      addVariablesColumn.toDF(removeNameColumnMetric: _*)

  }


  def computeCorrelationMatrix(
                                dataInit: DataFrame,
                                attributes: List[String],
                              ): DataFrame = {
    val headerDataUse = dataInit.columns.toList
    val intersectionHeaderAttributes = headerDataUse.intersect(attributes)
    val listDifference = attributes.filterNot(headerDataUse.contains)

    val attributeChecked = intersectionHeaderAttributes.nonEmpty match {
      case true => intersectionHeaderAttributes
      case false =>
        logger.error(
          "These attributes are not part of the variable names: " + listDifference.mkString(",")
        )
        intersectionHeaderAttributes
    }


    val colRenamed: List[String] = "variableName" :: attributeChecked
    println(colRenamed)

    val metrics: List[Column] = attributeChecked.flatMap(nameCol1 => attributeChecked.map(nameCol2 => corr(nameCol1,nameCol2)))

    val metricFrame: DataFrame = dataInit.agg(metrics.head, metrics.tail: _*)

    val matrixMetric: DataFrame  =
      attributeChecked
        .map(x => regroupCorrelationByVariable(x, metricFrame))
        .reduce(_.union(_))

    matrixMetric.select(colRenamed.head, colRenamed.tail: _*)



  }

}
