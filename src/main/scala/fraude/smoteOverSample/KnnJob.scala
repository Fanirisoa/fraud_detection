package fraude.smoteOverSample

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._


object KnnJob extends StrictLogging {

  def featureAssembler(dataFinal: DataFrame,
                       colList: List[String],
                       colLabel : String
                      ): DataFrame= {
    val assembler = new VectorAssembler()
      .setInputCols(colList.toArray)
      .setOutputCol("feature")

    val colFinal : List[String]= List(colLabel,"feature")

    assembler.transform(dataFinal)
      .select(colFinal.head, colFinal.tail: _*)
  }


   def featureDiAssembler(dataFinal: DataFrame,
                         colList: List[String],
                        ): DataFrame= {

     val colLength: Int = colList.length

    def convertVectorToArray: UserDefinedFunction = udf((features: Vector) => features.toArray)

    // Add a ArrayType Column
    val dfArr: DataFrame = dataFinal.withColumn("featuresArr" , convertVectorToArray(dataFinal("feature")))


    dfArr.select(col("*") +: (0 until colLength-1).map(i => column("featuresArr").getItem(i).as(colList(i))): _*).drop("featuresArr","feature")

  }






  def KNNCalculation(
                      dataFinal: DataFrame,
                      feature:String,
                      label: String,
                      reqrows:Int,
                      BucketLength:Int,
                      NumHashTables:Int): DataFrame= {
    val b1: DataFrame = dataFinal.withColumn("index", row_number().over(Window.partitionBy(label).orderBy(label)))
    val brp: BucketedRandomProjectionLSH = new BucketedRandomProjectionLSH().setBucketLength(BucketLength).setNumHashTables(NumHashTables).setInputCol(feature).setOutputCol("values")
    val model = brp.fit(b1)

    val transformedA = model.transform(b1)
    val transformedB = model.transform(b1)
    val b2 = model.approxSimilarityJoin(transformedA, transformedB, 20000000.0)
    require(b2.count > reqrows, println("Change bucket lenght or reduce the percentageOver"))
    b2.selectExpr("datasetA.index as id1",
      "datasetA.feature as k1",
      "datasetB.index as id2",
      "datasetB.feature as k2",
      "distCol").filter("distCol>0.0").orderBy("id1", "distCol").dropDuplicates().limit(reqrows)
  }

}