package fraude.XGBoostJOB

import com.typesafe.scalalogging.StrictLogging
import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame

object XGBoost extends StrictLogging {

  case class ParamXGBoostClassifier(eta : Float, max_depth : Int, objective : String, num_class : Long, num_round:  Long, num_workers: Int)


  def boosterPipeline(
                        splitLevel : Double,
                        listColFeatures : Array[String],
                        nameColClass : String,
                        inputDataFrame: DataFrame,
                        paramClassifier : ParamXGBoostClassifier
                      ): (XGBoostClassifier, Pipeline, DataFrame, DataFrame) = {


    // Split training and test dataset:
    val Array(training, test) = inputDataFrame.randomSplit(Array(splitLevel, 1 - splitLevel), 123)


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
    val booster: XGBoostClassifier = new XGBoostClassifier(
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
    val Pipeline = new Pipeline()
      .setStages(Array(assembler, labelIndexer, booster, labelConverter))

    (booster,Pipeline, training, test)
  }

  def xGBoostSimplePrediction(
                         splitLevel : Double,
                         listColFeatures : Array[String],
                         nameColClass : String,
                         inputDataFrame: DataFrame,
                         paramClassifier : ParamXGBoostClassifier
                       ): DataFrame= {


    val modelPipeline= boosterPipeline( splitLevel,
                                 listColFeatures,
                                 nameColClass,
                                 inputDataFrame,
                                 paramClassifier
                               )
    val pipeline: Pipeline = modelPipeline._2
    val training: DataFrame = modelPipeline._3
    val test: DataFrame = modelPipeline._4

    val model: PipelineModel = pipeline.fit(training)

    model.transform(test)
  }


  def evalPrediction(prediction: DataFrame
                      ): Double= {
    // Model evaluation
    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setLabelCol("classIndex")
    evaluator.setPredictionCol("prediction")

    evaluator.evaluate(prediction)

  }



  def xGBoostcrossValTune(
                           splitLevel : Double,
                           listColFeatures : Array[String],
                           nameColClass : String,
                           inputDataFrame: DataFrame,
                           paramClassifier : ParamXGBoostClassifier,
                           maxDepthGrid : Array[Int],
                           etaGrid: Array[Double],
                           foldNum : Int
                         ): DataFrame = {

    val modelPipeline= boosterPipeline( splitLevel,
      listColFeatures,
      nameColClass,
      inputDataFrame,
      paramClassifier
    )
    val booster: XGBoostClassifier = modelPipeline._1
    val pipeline: Pipeline = modelPipeline._2
    val training: DataFrame = modelPipeline._3
    val test: DataFrame = modelPipeline._4


    val model: PipelineModel = pipeline.fit(training)
    val prediction: DataFrame = model.transform(test)

    // Model evaluation
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
    evaluator.setLabelCol("classIndex")
    evaluator.setPredictionCol("prediction")


    // Tune paramGrid
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(booster.maxDepth, maxDepthGrid)
      .addGrid(booster.eta, etaGrid)
      .build()


    val cv: CrossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(foldNum)

    val cvModel: CrossValidatorModel = cv.fit(training)

    val bestModel: ParamXGBoostClassifier = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2)
      .asInstanceOf[ParamXGBoostClassifier]

    // Export the XGBoostClassificationModel as local XGBoost model,
    val goodModel: DataFrame = xGBoostSimplePrediction(
                                            splitLevel,
                                            listColFeatures,
                                            nameColClass,
                                            inputDataFrame,
                                            bestModel
                                          )

    goodModel
  }


}


