package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.stat.test.PatchedTestResult

class KSTwoSampleTestResult(override val statistic: Double,
                            override val pValue: Double,
                            override val nullHypothesis: String) extends PatchedTestResult {
  override def toString(): String ={
    "KS two sample test summary: \n" +
    super.toString()
  }

}