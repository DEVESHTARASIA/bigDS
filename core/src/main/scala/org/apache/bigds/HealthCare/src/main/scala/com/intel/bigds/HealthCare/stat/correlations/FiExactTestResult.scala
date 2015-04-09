package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.stat.test.PatchedTestResult


class FiExTestResult private[stat] (override val pValue: Double, val oddsRatio: Double,
                                         override val nullHypothesis: String,
                                           override val statistic: Double = 0) extends PatchedTestResult {

  override def toString: String = {
    "Fisher exact test summary:\n" +
    super.toString
  }
}

