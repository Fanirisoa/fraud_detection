package fraude.XGBoostJOB

import com.typesafe.scalalogging.StrictLogging
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object XGBoostCVTune extends StrictLogging {


  def xGBoostcrossValTune(
                           splitLevel : Double,
                           listColFeatures : Array[String],
                           nameColClass : String,
                           inputDataFrame: DataFrame
                         ): DataFrame = {

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


    // Convert indexed double label back to original string label:
    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("realLabel")
      .setLabels(labelIndexer.labels)


    // Create default param map for XGBoost
    val paramMapInitial = Map(
      "eta" -> 0.1f,
      "maxDepth" -> 2,
      "gamma" -> 0.0,
      "colsample_bylevel" -> 1,
      "objective" -> "multi:softprob",
      "num_class" -> 3,
      "booster" -> "gbtree",
      "numRound" -> 100,
      "num_workers" -> 3
    )


    // Use XGBoostClassifier to train classification model:
    val booster: XGBoostClassifier = new XGBoostClassifier(paramMapInitial)
    booster.setFeaturesCol("features")
    booster.setLabelCol("classIndex")


    // Tune paramGrid
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(booster.eta, Array(0.015))
      .addGrid(booster.maxDepth, Array(16))
      .addGrid(booster.numRound, Array(1000))
      .addGrid(booster.maxBins, Array(2))
      .addGrid(booster.minChildWeight, Array(0.2))
      .addGrid(booster.alpha, Array(0.8, 0.9))
      .addGrid(booster.lambda, Array(0.9, 1.0))
      .addGrid(booster.subsample, Array(0.6, 0.65, 0.7))
      .build()


    // Create the XGBoost pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, labelIndexer, booster, labelConverter))


    // Setup the classifier evaluation
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
                  .setLabelCol("classIndex")
                  .setPredictionCol("prediction")


    val cv: CrossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val cvModel: CrossValidatorModel = cv.fit(training)

    val results: DataFrame = cvModel.transform(test)

    results
  }


}


