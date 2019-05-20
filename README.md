# ingestYelpData

######  We need to set the environment variables to the path of the spark 


    SPARK_CONF_DIR=/usr/local/Cellar/spark-2.4.1-bin-hadoop2.7/conf
    
    
######  We need to put the data in the input path in HDFS :

- Create the yelp directory in HDFS :
    
    
        $ hdfs dfs -mkdir /tmp/incoming
        $ hdfs dfs -mkdir /tmp/incoming/yelp
    

- Insert the data in `/tmp/incoming/yelp`


        hdfs dfs -put /Users/fanirisoa/Documents/allData/yelp/  /tmp/incoming/yelp/
    



    