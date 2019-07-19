package fraude.smoteOverSample

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.{UserDefinedFunction}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import fraude.smoteOverSample.KnnJob._

object smoteClass extends StrictLogging {



  def smoteCalc(key1: org.apache.spark.ml.linalg.Vector, key2: org.apache.spark.ml.linalg.Vector): Array[Vector] ={
    val resArray: Array[Vector] = Array(key1, key2)

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

    // Generate the Array of features
    val dataAssembly: DataFrame =  featureAssembler(inputFrame,featureList,label)
    // Count the number of occurrences of each class or categories
    val groupedData = inputFrame.groupBy(label).count

    // Test to check if the number of the class equal to 2
    require(groupedData.count == 2, println("Only 2 labels allowed"))

    // To get all the distinct count for each class
    val classAll: Array[Row] = groupedData.collect()

    // Returne the value of the class with the minority class
    val minorityclass: String = if (classAll(0)(1).toString.toInt > classAll(1)(1).toString.toInt) classAll(1)(0).toString else classAll(0)(0).toString

    // Select alle the row  associate to the minority class
    val frame: Dataset[Row] = dataAssembly.select(col("feature"),col(label)).where(label + " == " + minorityclass)

    // Numbre of  element in the minority
    val rowCount: Long = frame.count
    // Number of instance to create
    val reqrows: Int = (rowCount * (percentOver/100)).toInt

    // Using KNN method
    val md: UserDefinedFunction = udf(smoteCalc _)
    val b1: DataFrame = KNNCalculation(frame, "feature",label, reqrows, BucketLength, NumHashTables)

    // Filter and add new instances
    val b2: DataFrame = b1.withColumn("ndtata", md(col("k1"), col("k2"))).select("ndtata")
    val b3: Dataset[Row] = b2.withColumn("feature", explode(col("ndtata"))).select("feature").dropDuplicates
println(b3.count())

    /*
    b3.show()
    b3.printSchema()

    def convertVectorToArray: UserDefinedFunction = udf((features: Vector) => features.toArray)
    val dfArr: DataFrame = b3.withColumn("featuresArr" , convertVectorToArray(b3("feature")))
    dfArr.show()
    dfArr.printSchema()

     */

    val b4: DataFrame= b3.withColumn(label, lit(minorityclass).cast(frame.schema(1).dataType)).select("Class","feature")

    dataAssembly.union(b4).dropDuplicates
  }


}