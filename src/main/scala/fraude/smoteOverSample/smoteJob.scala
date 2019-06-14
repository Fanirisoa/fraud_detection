package fraude.smoteOverSample

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

object smoteClass extends StrictLogging {

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




  def smoteCalc(key1: org.apache.spark.ml.linalg.Vector, key2: org.apache.spark.ml.linalg.Vector): Array[linalg.Vector] ={
    val resArray: Array[linalg.Vector] = Array(key1, key2)

    val resZip: Array[(Double, Double)] = key2.toArray.zip(key1.toArray)

    val res: Array[Double] = key1.toArray.zip(resZip.map(x => x._1 - x._2).map(_*0.2)).map(x => x._1 + x._2)
    resArray :+ org.apache.spark.ml.linalg.Vectors.dense(res)
  }


  def Smote(
             inputFrame:org.apache.spark.sql.DataFrame,
             featureList:List[String],
             label:String,
             percentOver:Int,
             BucketLength:Int,
             NumHashTables:Int): DataFrame = {

    val dataAssembly: DataFrame =  featureAssembler(inputFrame,featureList,label)
    val groupedData = inputFrame.groupBy(label).count
    println(groupedData.count)


    println("OK 1")

    require(groupedData.count == 2, println("Only 2 labels allowed"))
    val classAll: Array[Row] = groupedData.collect()

    classAll.foreach(println)


    val minorityclass: String = if (classAll(0)(1).toString.toInt > classAll(1)(1).toString.toInt) classAll(1)(0).toString else classAll(0)(0).toString

    println(minorityclass)

    println("OK 2")

    val frame: Dataset[Row] = dataAssembly.select(col("feature"),col(label)).where(label + " == " + minorityclass)

    frame.show()


    val rowCount: Long = frame.count
    println(rowCount)

    val reqrows: Int = (rowCount * (percentOver/100)).toInt
    println(rowCount * (percentOver/100))
    println(reqrows)


    val md: UserDefinedFunction = udf(smoteCalc _)
    val b1: DataFrame = KNNCalculation(frame, "feature",label, reqrows, BucketLength, NumHashTables)


    val b2: DataFrame = b1.withColumn("ndtata", md(col("k1"), col("k2"))).select("ndtata")
    val b3: Dataset[Row] = b2.withColumn("feature", explode(col("ndtata"))).select("feature").dropDuplicates
    val b4: DataFrame= b3.withColumn(label, lit(minorityclass).cast(frame.schema(1).dataType)).select("Class","feature")
    println("OK 3")

    b4.show(20)
    dataAssembly.show(20)

    b4.printSchema()
    dataAssembly.printSchema()


    dataAssembly.union(b4).dropDuplicates
  }


}