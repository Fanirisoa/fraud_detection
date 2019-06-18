package fraude.XGBoostJOB

import com.typesafe.scalalogging.StrictLogging
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object XGBoost extends StrictLogging {

  case class ParamXGBoostClassifier(eta : Float, max_depth : Int, objective : String, num_class : Long, num_round:  Long, num_workers: Int)


  def XGBoostPrediction(
                        splitLevel : Double,
                        listColFeatures : Array[String],
                        nameColClass : String,
                        inputDataFram: DataFrame,
                        paramClassifier : ParamXGBoostClassifier
                      ): DataFrame= {


    // Split training and test dataset:
    val Array(training, test) = inputDataFram.randomSplit(Array(splitLevel, 1- splitLevel), 123)


    // Assemble all features into a single vector column :
    val assembler = new VectorAssembler()
      .setInputCols(listColFeatures)
      .setOutputCol("features")

    // From string label to indexed double label
    val labelIndexer = new StringIndexer()
      .setInputCol(nameColClass)
      .setOutputCol("classIndex")
      .fit(training)


    // Use XGBoostClassifier to train classification model:
    val booster = new XGBoostClassifier(
      Map("eta" -> paramClassifier.eta,
        "max_depth" -> paramClassifier.max_depth,
        "objective" -> paramClassifier.objective,
        "num_class" -> paramClassifier.num_class,
        "num_round" -> paramClassifier.num_round,
        "num_workers" -> paramClassifier.num_workers
      )
    )
    booster.setFeaturesCol("features")
    booster.setLabelCol("classIndex")


    // Convert indexed double label back to original string label:
    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("realLabel")
      .setLabels(labelIndexer.labels)

    // Pipeline
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(assembler, labelIndexer, booster, labelConverter))
    val model: PipelineModel = pipeline.fit(training)

    model.transform(test)
  }


}


