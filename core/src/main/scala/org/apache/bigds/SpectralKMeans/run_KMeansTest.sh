$SPARK_HOME/bin/spark-submit -v --executor-memory 160g --driver-memory 170g --class "com.Intel.bigDS.clustering.KMeansTest" --master spark://sr471:7180 ./target/spectralkmeans.jar spark://sr471:7180 hdfs://sr471:54311/user/chunnan/cluster_data/512000x64/numerical.csv/ 224 0.3 1 640

