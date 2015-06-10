package com.intel.bigds.HealthCare.stat

import breeze.linalg.{DenseMatrix => BDM, max}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkException, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


import scala.collection.mutable

import org.apache.commons.math3.distribution.HypergeometricDistribution

import scala.util.Random


object FiExact {

  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val dependence = Value("the occurrence of the outcomes is statistically dependent.")
    val independence = Value("the occurrence of the outcomes is statistically independent.")
  }


  //FIXME:will need mxn support of fisher's exact test. Please refer to the network algorithm implemented in R
  //when methodName is 2x2, only fisher's exact test is conducted. If there are more than 2 labels or categories, an error will be threw
  //when methodName is mxn, fisher's exact test is conducted when there are 2 labels and categories, otherwise use chi-sqare approximate test
  def FiExactFeatures(data: RDD[LabeledPoint], methodName: String = "2x2"): Array[FiExTestResult] = {
    val maxCategories = 10000
    val numCols = data.first().features.size
    val results = new Array[FiExTestResult](numCols)
    var labels: Map[Double, Int] = null
    // at most 1000 columns at a time
    val batchSize = 1000
    var batch = 0
    while (batch * batchSize < numCols) {
      // The following block of code can be cleaned up and made public as
      // chiSquared(data: RDD[(V1, V2)])
      val startCol = batch * batchSize
      val endCol = startCol + math.min(batchSize, numCols - startCol)
      val pairCounts = data.mapPartitions { iter =>
        val distinctLabels = mutable.HashSet.empty[Double]
        val allDistinctFeatures: Map[Int, mutable.HashSet[Double]] =
          Map((startCol until endCol).map(col => (col, mutable.HashSet.empty[Double])): _*)
        var i = 1
        iter.flatMap { case LabeledPoint(label, features) =>
          if (i % 1000 == 0) {
            if (distinctLabels.size > maxCategories) {
              throw new SparkException(s"FiExact test expect factors (categorical values) but "
                + s"found more than $maxCategories distinct label values.")
            }
            allDistinctFeatures.foreach { case (col, distinctFeatures) =>
              if (distinctFeatures.size > maxCategories) {
                throw new SparkException(s"FiExact test expect factors (categorical values) but "
                  + s"found more than $maxCategories distinct values in column $col.")
              }
            }
          }
          i += 1
          distinctLabels += label
          features.toArray.view.zipWithIndex.slice(startCol, endCol).map { case (feature, col) =>
            allDistinctFeatures(col) += feature
            (col, feature, label)
          }
        }
      }.countByValue()

      if (labels == null) {
        // Do this only once for the first column since labels are invariant across features.
        labels =
          //pairCounts.keys.filter(_._1 == startCol).map(_._3).toArray.distinct.zipWithIndex.toMap
          pairCounts.keys.filter(_._1 == startCol).map(_._3).toArray.distinct.zipWithIndex.toMap
      }

      val numLabels = labels.size
      if (methodName == "2x2") {
        pairCounts.keys.groupBy(_._1).map { case (col, keys) =>
          val features = keys.map(_._2).toArray.distinct.zipWithIndex.toMap
          val numRows = features.size
          val contingency = new BDM[Int](numRows, numLabels, new Array[Int](numRows * numLabels))
          keys.foreach { case (_, feature, label) =>
            val i = features(feature)
            val j = labels(label)
            contingency(i, j) += pairCounts((col, feature, label)).toInt
          }
          println("============input matrix=============")
          println(contingency)
          results(col) = FiExactMatrix(contingency)
        }
      }
      else {
        //when methodName is mxn, fisher's exact test is conducted when there are 2 labels and categories, otherwise use chi-sqare approximate test
        //waiting to be added.
        /* if (numLabels == 2){
          pairCounts.keys.groupBy(_._1).map { case (col, keys) =>
            val features = keys.map(_._2).toArray.distinct.zipWithIndex.toMap
            val numRows = features.size
            if (numRows == 2){
              val contingency = new BDM(numRows, numLabels, new Array[Int](numRows * numLabels))
              keys.foreach { case (_, feature, label) =>
                val i = features(feature)
                val j = labels(label)
                contingency(i, j) += pairCounts((col, feature, label))
              }
              results(col) = FiExactMatrix(contingency)
            }
            else {
              results(col) = ChiSqTest.chiSquaredMatrix(Matrices.fromBreeze(contingency), methodName)
            }
          }
        } */
      }

      batch += 1
    }
    results

  }

  //ported from scipy.stats
 /* """Performs a Fisher exact test on a 2x2 contingency table.

  Parameters
  ----------
  table : array_like of ints
  A 2x2 contingency table.  Elements should be non-negative integers.
  alternative : {'two-sided', 'less', 'greater'}, optional
  Which alternative hypothesis to the null hypothesis the test uses.
  Default is 'two-sided'.

  Returns
  -------
  oddsratio : float
  This is prior odds ratio and not a posterior estimate.
  p_value : float
  P-value, the probability of obtaining a distribution at least as
  extreme as the one that was actually observed, assuming that the
  null hypothesis is true.

  See Also
    --------
  chi2_contingency : Chi-square test of independence of variables in a
    contingency table.

  Notes
  -----
  The calculated odds ratio is different from the one R uses. In R language,
  this implementation returns the (more common) "unconditional Maximum
  Likelihood Estimate", while R uses the "conditional Maximum Likelihood
  Estimate".

    For tables with large numbers the (inexact) chi-square test implemented
    in the function `chi2_contingency` can also be used.

    Examples
    --------
    Say we spend a few days counting whales and sharks in the Atlantic and
    Indian oceans. In the Atlantic ocean we find 8 whales and 1 shark, in the
    Indian ocean 2 whales and 5 sharks. Then our contingency table is::

    Atlantic  Indian
    whales     8        2
    sharks     1        5

    We use this table to find the p-value:

    >>> oddsratio, pvalue = stats.fisher_exact([[8, 2], [1, 5]])
    >>> pvalue
    0.0349...

    The probability that we would observe this or an even more imbalanced ratio
    by chance is about 3.5%.  A commonly used significance level is 5%, if we
    adopt that we can therefore conclude that our observed imbalance is
    statistically significant; whales prefer the Atlantic while sharks prefer
    the Indian ocean.*/
  def FiExact_TwoSample(col1: Array[Int], col2: Array[Int], alternative: String = "two-sided") : FiExTestResult = {
    val len1 = col1.length
    val len2 = col2.length
    if (len1 != len2) {
      throw new IllegalArgumentException("\"observed and expected must be of the same size.")
    }
    val contingency = new BDM[Int](len1, 2, new Array[Int](2 * len1))
    FiExactMatrix(contingency, alternative)
  }

  def FiExactMatrix(counts: BDM[Int], alternative: String = "two-sided"): FiExTestResult = {
    val numRows = counts.rows
    val numCols = counts.cols
    var pvalue = 0.0
    var oddsratio = 0.0
    if (numRows != 2 || numCols != 2) {
      throw new IllegalArgumentException("Contingency table must be 2x2. ")
    }
    val data_items = counts.toArray
    if ((data_items(0) < 0) || (data_items(1) < 0) || data_items(2) < 0 || data_items(3) < 0) {
      throw new IllegalArgumentException("All values in `table` must be nonnegative.")
    }

    if ((data_items(0) == 0 && data_items(1) == 0 || data_items(2) == 0 && data_items(3) == 0) || (data_items(0) == 0 && data_items(2) == 0 || data_items(1) == 0 && data_items(3) == 0)) {
      pvalue = 1.0
      oddsratio = Double.NaN
      new FiExTestResult(1.0, oddsratio, NullHypothesis.independence.toString())
    }

    if (counts(1, 0) > 0 && counts(0, 1) > 0) {
      oddsratio = counts(0, 0) * counts(1, 1) / (counts(1, 0) * counts(0, 1)).toFloat
    }
    else {
      oddsratio = Double.PositiveInfinity
    }
    val n1 = counts(0, 0) + counts(0, 1)
    val n2 = counts(1, 0) + counts(1, 1)
    val n = counts(0, 0) + counts(1, 0)
    val hypergeo = new HypergeometricDistribution(n1 + n2, n1, n)

    var mode: Int = 0
    var pexact: Double = 0.0
    var pmode: Double = 0.0
    val epsilon = 1 - 1e-4
    if (alternative == "less") {
      pvalue = hypergeo.cumulativeProbability(counts(0, 0))
    }
    else if (alternative == "greater") {
      val hypergeo_2 = new HypergeometricDistribution(n1 + n2, n1, counts(0, 1) + counts(1, 1))
      pvalue = hypergeo_2.cumulativeProbability(counts(0, 1))
    }
    else if (alternative == "two-sided") {
      mode = ((n + 1) * (n1 + 1).toFloat / (n1 + n2 + 2)).toInt
      pexact = hypergeo.probability(counts(0, 0))
      pmode = hypergeo.probability(mode)


      if ((pexact - pmode).abs / max(pexact, pmode) <= 1 - epsilon) {
        pvalue = 1.0
        new FiExTestResult(pvalue, oddsratio, NullHypothesis.independence.toString)
      }
      else if (counts(0, 0) < mode) {
        val plower = hypergeo.cumulativeProbability(counts(0, 0))
        if (hypergeo.probability(n) > pexact / epsilon) {
          new FiExTestResult(plower, oddsratio, NullHypothesis.independence.toString)
        }
        val guess = binary_search(n, n1, n2, "upper")
        pvalue = plower + 1 - hypergeo.cumulativeProbability(guess - 1)
      }
      else {
        val pupper = 1-hypergeo.cumulativeProbability(counts(0, 0) - 1)
        if (hypergeo.probability(0) > pexact / epsilon) {
          new FiExTestResult(pupper, oddsratio, NullHypothesis.independence.toString)
        }
        val guess = binary_search(n, n1, n2, "lower")
        pvalue = pupper + hypergeo.cumulativeProbability(guess)
      }
    }
    else {
      throw new IllegalArgumentException("`alternative` should be one of {'two-sided', 'less', 'greater'}")
    }
    if (pvalue > 1.0) {
      pvalue = 1.0
    }

//Binary search for where to begin lower/upper halves in two-sided test.
    def binary_search(n: Int, n1: Int, n2: Int, side: String): Int = {
      var minval: Int = 0
      var maxval: Int = 0
      if (side == "upper") {
        minval = mode
        maxval = n
      }
      else {
        minval = 0
        maxval = mode
      }
      var guess = -1
      var break_indicate = true
      while ((maxval - minval) > 1 && break_indicate) {
        if ((maxval == maxval + 1) && (guess == minval)) {
          guess = maxval
        }
        else {
          guess = (maxval + minval) / 2
        }
        val pguess = hypergeo.probability(guess)
        var ng: Int = 0
        if (side == "upper") {
          ng = guess - 1
        }
        else {
          ng = guess + 1
        }
        if (pguess <= pexact && hypergeo.probability(ng) > pexact) {
          break_indicate = false //same as break
        }
        else if (pguess < pexact) {
          maxval = guess
        }
        else {
          minval = guess
        }
      }
      if (guess == -1) {
        guess = minval
      }
      if (side == "upper") {
        while (guess > 0 && hypergeo.probability(guess) < pexact * epsilon) {
          guess -= 1
        }
        while (hypergeo.probability(guess) > pexact / epsilon) {
          guess += 1
        }
      }
      else {
        while (hypergeo.probability(guess) < pexact * epsilon) {
          guess += 1
        }
        while (guess > 0 && hypergeo.probability(guess) > pexact / epsilon) {
          guess -= 1
        }
      }
      guess
    }
    new FiExTestResult(pvalue, oddsratio, NullHypothesis.independence.toString)

    //val hypergeom = new HypergeometricDistribution()
  }
}



