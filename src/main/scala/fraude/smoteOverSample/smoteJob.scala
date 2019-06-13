package fraude.smoteOverSample

import com.typesafe.scalalogging.StrictLogging

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object smoteClass extends StrictLogging {

  def featureAssembler(dataFinal: DataFrame,
                       colList: List[String]
                    ): DataFrame= {
    val assembler = new VectorAssembler()
      .setInputCols(colList.toArray)
      .setOutputCol("feature")

    assembler.transform(dataFinal)
  }





  def KNNCalculation(
                      dataFinal: DataFrame,
                      feature:String,
                      reqrows:Int,
                      BucketLength:Int,
                      NumHashTables:Int): DataFrame= {
    val b1: DataFrame = dataFinal.withColumn("index", row_number().over(Window.partitionBy("label").orderBy("label")))
    val brp: BucketedRandomProjectionLSH = new BucketedRandomProjectionLSH().setBucketLength(BucketLength).setNumHashTables(NumHashTables).setInputCol(feature).setOutputCol("values")
    val model = brp.fit(b1)

    val transformedA = model.transform(b1)
    val transformedB = model.transform(b1)
    val b2 = model.approxSimilarityJoin(transformedA, transformedB, 2000000000.0)
    require(b2.count > reqrows, println("Change bucket lenght or reduce the percentageOver"))
    b2.selectExpr("datasetA.index as id1",
      "datasetA.feature as k1",
      "datasetB.index as id2",
      "datasetB.feature as k2",
      "distCol").filter("distCol>0.0").orderBy("id1", "distCol").dropDuplicates().limit(reqrows)

  }









}