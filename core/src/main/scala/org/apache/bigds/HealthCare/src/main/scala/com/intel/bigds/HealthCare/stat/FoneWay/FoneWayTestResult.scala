
package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.stat.test.PatchedTestResult


class FoneWayTestResult private[stat] (override val statistic: Double,
                                       val pValue: Double,
                                       override val degreesOfFreedom: Int,
                                       val withindegreesOfFreedom: Int,
                                       override val nullHypothesis: String ) extends PatchedTestResult[Int] {

  override def toString: String = {

    val pValueExplain = if (pValue <= 0.01) {
      s"Very strong presumption against null hypothesis: $nullHypothesis. \n"
    } else if (0.01 < pValue && pValue <= 0.05) {
      s"Strong presumption against null hypothesis: $nullHypothesis. \n"
    } else if (0.05 < pValue && pValue <= 0.1) {
      s"Low presumption against null hypothesis: $nullHypothesis. \n"
    } else {
      s"No presumption against null hypothesis: $nullHypothesis. \n"
    }

    s"The computed F-value of the test is $statistic \n" +
    s"The associated p-value from the F-distribution is $pValue \n" +
    s"The between-group degrees of freedom is $degreesOfFreedom \n" +
    s"The within-group degrees of freedom is $withindegreesOfFreedom"
  }


}