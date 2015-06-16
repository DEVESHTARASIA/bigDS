$SPARK_HOME/bin/spark-submit -v --executor-memory 160g --driver-memory 170g --conf "spark.driver.maxResultSize=15g" --class "com.Intel.bigDS.clustering.SkLSHTest" --master spark://sr471:7180 ./target/spectralkmeans.jar spark://sr471:7180 hdfs://sr471:54311/user/chunnan/cluster_data/128000x64/numerical.csv/ 224 0.0 1 160 0.002


