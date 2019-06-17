# fraud_detection

### Paths 

    inputPath = "/tmp/incoming/fraudDetection/"
    savePath = "/tmp/fraudResults/"

######  We need to set the environment variables to the path of the spark 

If I want to use local path as initial path, I use the following environment :

    
    SPARK_CONF_DIR=/Library/spark-2.1.2/
    

If I want to use HDFS path as initial path, I use the following environment :

    
    SPARK_CONF_DIR=/usr/local/Cellar/spark-2.4.1-bin-hadoop2.7/conf
    

    
######  We need to put the data in the input path in HDFS :

- Create the allData directory in HDFS :
    
    
        $ hdfs dfs -mkdir /tmp/incoming/fraudDetection/
    

- Insert the data in `/Users/fanirisoa/Documents/allData/fraud_detection`


        hdfs dfs -put /Users/fanirisoa/Documents/allData/fraud_detection/  /tmp/incoming/fraudDetection/
    
    
    
    
- Delete the saving path it is already exist

        
       hdfs dfs -rmr /tmp/testResults/testMetricTitanic /tmp/testResults/testMetricIris /tmp/testResults/testMetricTafeng
  
  
    
    
    
    
    
    

#### References :
- [Séries temporelles de détection d'anomalies de prix](https://supportivy.com/series-temporelles-de-detection-danomalies-de-prix-vers-la-science-des-donnees/).
- [Adaptive Machine Learning for Credit Card Fraud Detection](http://di.ulb.ac.be/map/adalpozz/pdf/Dalpozzolo2015PhD.pdf).
- [Détection de la fraude financière à l’aide de l’apprentissage automatique](http://www.mbenhamd.com/var/f/vq/ve/vqve0tewBifvp1QcJ3zlPnDj2EL9gXRsoKry5uA-ZMY_master.pdf).
- [Fraud Detection with SMOTE and XGBoost in R](https://www.kaggle.com/bonovandoo/fraud-detection-with-smote-and-xgboost-in-r).
- [Predicting Fraud in Financial Payment Services](https://www.kaggle.com/arjunjoshua/predicting-fraud-in-financial-payment-services).
- [Financial Fraud Detection-XGBoost](https://www.kaggle.com/georgepothur/4-financial-fraud-detection-xgboost).
- [TalkingData AdTracking Fraud Detection Challenge](https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/discussion/56777#latest-329919).
- [Reinforcement Learning with Python](https://towardsdatascience.com/reinforcement-learning-with-python-8ef0242a2fa2).


- [Reinforcement Learning with Python](https://xgboost.readthedocs.io/en/latest/tutorials/model.html).
- [new BucketedRandomProjectionLSH()](https://spark.apache.org/docs/latest/ml-features.html#bucketed-random-projection-for-euclidean-distance).
- [sparkSubmitSmote.scala](https://github.com/Angkirat/Smote-for-Spark/blob/master/sparkSubmitSmote.scala).
- [SMOTE.scala](https://github.com/anathan90/SparkSMOTE/blob/master/src/main/scala/SMOTE.scala).
- [new BucketedRandomProjectionLSH()](https://towardsdatascience.com/methods-for-dealing-with-imbalanced-data-5b761be45a18).
- [new BucketedRandomProjectionLSH()](https://towardsdatascience.com/having-an-imbalanced-dataset-here-is-how-you-can-solve-it-1640568947eb).
- [new BucketedRandomProjectionLSH()](https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html).https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html).
-[Introduction to Boosted Trees](https://xgboost.readthedocs.io/en/latest/tutorials/model.html).
-[Awesome XGBoost](https://github.com/dmlc/xgboost/tree/master/demo).
-[Complete Guide to Parameter Tuning in XGBoost ](https://www.analyticsvidhya.com/blog/2016/03/complete-guide-parameter-tuning-xgboost-with-codes-python/).































































