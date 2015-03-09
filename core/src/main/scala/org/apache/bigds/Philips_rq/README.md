Health Care Support For BigDS
=======

According to Philips requirements, we have implemented:

	1. Chi-Square Two-sample Test, including calculation fo Cramer's V
	2. Wilcox Rank-Sum Test
	3. Association Rule Mining (FP-Growth)
	4. Fisher's exact test
	5. Missing value handling (fill with mean\median\proportional random)

Install
=======

You need to install Spark 1.2.0 or higher versions together with hadoop 1.0.4 as storage support.

Please set Spark's root as the environment variable on your computer, named SPARK_HOME

Building BigDS Healthcare support
----------------

Clone BigDS from github:

    cd [your_building_root]
    git git@github.com:intel-hadoop/bigDS.git

Go to the following directory:

    cd bigDS/core/src/main/scala/org/apache/bigds/Philips_rq
  
Build & package:

    sbt                  // in Philips_rq root
    package
   
Run tests
-----------

    Currently we have 5 main tests:

	 [1] com.intel.Philips.stat.ChisqwithData
	 [2] com.intel.Philips.stat.WilcoxonRankSum
	 [3] com.intel.Philips.stat.FiExactTest
	 [4] com.intel.Philips.stat.MissValueHandling
	 [5] com.intel.Philips.stat.AssociationAnalysis

    Please refer to the details of each test in their source code. 
    
    Please edit the Philips_rq/conf/Philips_rq-Depolyment first. Configuring spark master address and port, as well as the location of test data file. In default, we have a test data file sample_data.csv under Philips_rq/ref. 

    Under the Philips_rq directory, choose a shell script to run tests. For example: 
	./run_Chisq_test.sh

    Good luck!

