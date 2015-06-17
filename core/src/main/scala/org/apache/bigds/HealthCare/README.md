Health Care Support For BigDS
=======

This is a basic big data toolkit for health care implementations. We have implemented:

	1. Pair-wise Chi-Square Two-sample Test & Fisher's exact test, including calculation fo Cramer's V
	2. Wilcox Rank-Sum Test
	3. F one way Test (one way ANOVA)
        4. KS Test
	5. Missing value handling (fill with mean\median\proportional random)

Install
=======

You need to install Spark 1.3.0 or higher versions together with hadoop 1.0.4 as storage support.

Please set Spark's root as the environment variable on your computer, named SPARK_HOME

Building BigDS Healthcare support
----------------

Clone BigDS from github:

    cd [your_building_root]
    git git@github.com:intel-hadoop/bigDS.git

Go to the following directory:

    cd bigDS/core/src/main/scala/org/apache/bigds/HealthCare
  
Build & package:

    sbt                  // under HealthCare root
    package
   
Run tests
-----------

    Currently we have 8 main runnable programs:
	 [1] com.intel.bigds.HealthCare.example.KSTest_RDD_Scan
 	 [2] com.intel.bigds.HealthCare.generator.DataGenerator
  	 [3] com.intel.bigds.HealthCare.example.FoneWayTest
   	 [4] com.intel.bigds.HealthCare.example.KSTest
    	 [5] com.intel.bigds.HealthCare.example.MissValueHandling
     	 [6] com.intel.bigds.HealthCare.example.WilcoxonTest
      	 [7] com.intel.bigds.HealthCare.example.ChiSquareTest
         [8] com.intel.bigds.HealthCare.example.KSTest_Partition_br

    [2] is used to generate synthetic data. [1][4][8]are three ways to compute pair-wise KS test. We finally use block cartesian method in [4], so does wilcoxonTest.[5]can be run locally, reading local test data under ./ref/test_data.

    Please refer to the details of each test in their source code. 
    
    Please edit the ./conf/healthcare-deployment-conf first. Configuring spark master address and port, as well as the location of test data file. To run test queries, we need to use "DataGenerator" to generate synthetic data into given HDFS address.  

    Under the HealthCare/ directory, choose a shell script to run tests. For example: 
	./run_Chisq_test.sh

    Good luck!

