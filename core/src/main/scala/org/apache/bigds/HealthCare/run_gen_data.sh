. conf/healthcare-deployment-conf

${SPARK_HOME}/bin/spark-submit -v --executor-memory 160g --driver-memory 170g --class "com.intel.bigds.HealthCare.generator.DataGenerator" --master ${SPARK_MASTER} target/healthcare.jar ${SPARK_MASTER} ${HDFS_MASTER}${DATA_PATH} ${nParts_gen} ${nRecords} ${nFeatures} ${Numerical} ${Categorical} ${LostRatio} ${BlankItems}
