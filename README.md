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


- [new BucketedRandomProjectionLSH()](https://spark.apache.org/docs/latest/ml-features.html#bucketed-random-projection-for-euclidean-distance).


































































