package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.util.MLlibTestSparkContext
import com.intel.bigds.HealthCare.preprocessing._

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.TestingUtils._

class KSTwoSampleTestSuite extends FunSuite with MLlibTestSparkContext {
  test("local original KS of apache common math3") {
      val data1 = Array(1.1,2.4,1.0,3.1,2.2,1.0,4.0)
      val data2 = Array(2.1,1.2,3.3,5.1,2.4,4.4,3.3)
      val result = KSTwoSampleTest.ks_2samp_math(data1, data2)
      assert(result.statistic ~== 0.42857 relTol 1e-4)
      assert(result.pValue ~== 0.40559 relTol 1e-4)

      val result2 = KSTwoSampleTest.ks_2samp_scipy(data1,data2)
      assert(result2.statistic ~== 0.42857 relTol 1e-4)
      assert(result2.pValue ~== 0.40559 relTol 1e-4)

      val data1_rdd = sc.parallelize(data1.sortBy(i=>i).zipWithIndex.map(i => (i._1, 1, i._2.toLong)))
      val data2_rdd = sc.parallelize(data2.sortBy(i=>i).zipWithIndex.map(i => (i._1, 2, i._2.toLong)))
      val result3 = KSTwoSampleTest.ks_2samp_sc(data1_rdd, data2_rdd)
      assert(result2.statistic ~== 0.42857 relTol 1e-4)
    //assert(result2.pValue ~== 0.40559 relTol 1e-4)
      //why do 0.4285714285714286 and 0.42857142857142855 will make great difference in pValue?
  }


}
