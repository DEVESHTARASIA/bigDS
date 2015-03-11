package com.intel.bigds.stat

import org.apache.spark.annotation.Experimental

@Experimental
trait TestResult[DF] {

  /**
   * The probability of obtaining a test statistic result at least as extreme as the one that was
   * actually observed, assuming that the null hypothesis is true.
   */
  def pValue: Double

  def oddsRatio: Double

  /**
   * Null hypothesis of the test.
   */
  def nullHypothesis: String

  /**
   * String explaining the hypothesis test result.
   * Specific classes implementing this trait should override this method to output test-specific
   * information.
   */
  override def toString: String = {

    // String explaining what the p-value indicates.
    val pValueExplain = if (pValue <= 0.01) {
      s"Very strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.01 < pValue && pValue <= 0.05) {
      s"Strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.05 < pValue && pValue <= 0.1) {
      s"Low presumption against null hypothesis: $nullHypothesis."
    } else {
      s"No presumption against null hypothesis: $nullHypothesis."
    }
      s"pValue = $pValue \n"
      s"oddsRatio = $oddsRatio \n" + pValueExplain
  }
}

/**
 * :: Experimental ::
 * Object containing the test results for the chi-squared hypothesis test.
 */
@Experimental
class FiExTestResult private[stat] (override val pValue: Double,
                                    override val oddsRatio: Double,
                                     override val nullHypothesis: String) extends TestResult[Int] {

  override def toString: String = {
    "Fisher exact test summary:\n" +
      super.toString
  }
}

