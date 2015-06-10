
package com.intel.bigds.HealthCare.stat

import org.apache.spark.mllib.stat.test.PatchedTestResult


class FoneWayTestResult private[stat] (override val statistic: Double,
                                       override val pValue: Double,
                                       val degreesOfFreedom: Int,
                                       val withindegreesOfFreedom: Int,
                                       override val nullHypothesis: String ) extends PatchedTestResult {

  override def toString: String = {
    "F one-way test summary: \n" +
    s"The between-group degrees of freedom is $degreesOfFreedom \n" +
    s"The within-group degrees of freedom is $withindegreesOfFreedom \n" +
    super.toString
  }


}