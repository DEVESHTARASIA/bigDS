package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.stat.test.PatchedTestResult


class WilcoxRankSumTestResult private[stat] (override val statistic: Double,
                               override val pValue: Double,
                               override val nullHypothesis: String) extends PatchedTestResult {
  override def toString(): String = {
    "Wilcoxon rank sum test result: \n" +
    super.toString()
  }

}