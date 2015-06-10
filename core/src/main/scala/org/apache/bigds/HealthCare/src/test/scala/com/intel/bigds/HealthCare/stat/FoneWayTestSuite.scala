package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.util.MLlibTestSparkContext
import com.intel.bigds.HealthCare.preprocessing._

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.TestingUtils._
//import org.
/*

class FoneWayTestSuite extends FunSuite with MLlibTestSparkContext {

  test("test 1-way ANOVA") {
    val data = Seq(
    Array(1.1,2.4,1.0,3.1,2.2,1.0,4.0),
    Array(2.1,1.2,3.3,5.1,2.4,4.4,3.3),
    Array(9.0,1.0,2.2,1.3,4.1,2.2,5.1),
    Array(2.1,2.2,3.2,1.1,4.4,1.9,8.7),
    Array(0.5,0.9,2.4,0.8,0.4,2.2,3.9)
    )
    val data_rdd = sc.parallelize(data).map(i => i.map(j => j.toString()))
    val rdd_container = new DataContainer(data_rdd, Set("NAN","?"))
    val result = FoneWay.FoneWayTest(rdd_container)
    val pValue = result.pValue
    val statistic = result.statistic
    //check according to scipy.stats.f_oneway
    assert(pValue ~== 0.17768 relTol 1e-4)
    assert(statistic ~== 1.62343 relTol 1e-4)
  }

}
*/

