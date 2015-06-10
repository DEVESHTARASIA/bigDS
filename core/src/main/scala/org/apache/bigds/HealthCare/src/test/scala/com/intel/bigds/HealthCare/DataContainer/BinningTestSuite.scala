package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.util.MLlibTestSparkContext
import com.intel.bigds.HealthCare.preprocessing._

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.TestingUtils._

class BinningTestSuite extends FunSuite with MLlibTestSparkContext {
  test("Data Binning Test") {
    val data = Seq (
      Array(0.0,1.2,9.8,3.4),
      Array(1.2,8.8,7.1,2.2),
      Array(5.3,1.2,5.2,8.9),
      Array(1.1,2.3,2.2,5.7),
      Array(9.1,11.2,4.2,4.4),
      Array(1.1,2.5,3.5,9.9),
      Array(15.2,2.8,4.1,7.2)
    )
    val data_rdd = sc.parallelize(data.map(i=>i.map(_.toString)))
    val container = new DataContainer(data_rdd, Set("?"))
    val res = container.Binning()
    println(res.data.map(i => i.mkString(",")).collect.mkString("\n"))
    assert(true)

  }
}
