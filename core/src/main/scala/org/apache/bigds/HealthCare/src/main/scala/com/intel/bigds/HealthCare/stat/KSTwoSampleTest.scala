//sort and shuffle issues waiting to be tackled
package com.intel.bigds.HealthCare.stat

import breeze.linalg.max
import breeze.numerics.{sqrt, abs}
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest
import org.apache.spark.rdd.RDD


object KSTwoSampleTest extends Serializable {

//directly org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest
  def ks_2samp_math(data1:Array[Double], data2:Array[Double]): KSTwoSampleTestResult = {
    val ksTestModel  = new KolmogorovSmirnovTest()
    val statistic = ksTestModel.kolmogorovSmirnovStatistic(data1, data2)
    val prob = ksTestModel.approximateP(statistic, data1.length, data2.length)
    new KSTwoSampleTestResult(statistic, prob, "Two samples are statistically the same distribution.")
}


  //ported from scipy.stats
  def search_sorted(data:Array[Double], dataall:Array[Double]): Array[Int] = {
    def binary_search(array: Array[Double], target: Double, low: Int, high: Int):Int = {
      val middle = (low + high) / 2
      if(array(middle) > target) {
        if (middle - low == 1 && array(low) <= target) {
          return low + 1
        }
        if (middle -low == 1 && array(low) > target) return 0
        binary_search(array, target, low, middle)
      }
      else if(array(middle) < target) {
        if (high - middle == 1 && array(high) > target) {
          return middle + 1
        }
        if (high - middle ==1 && array(high) <= target) return high + 1
        binary_search(array, target, middle, high)
      }
      else{
        return middle + 1
      }
    }
    val result = for (i <- dataall) yield{
      binary_search(data, i, 0, data.length-1)
    }
    result.toArray

  }

  def ks_2samp_scipy(data1:Array[Double], data2:Array[Double]): KSTwoSampleTestResult = {
    val n1 = data1.length
    val n2 = data2.length
    val Data1 = data1.sortBy(i => i)
    val Data2 = data2.sortBy(i => i)
    val data_all = Data1 ++ Data2
    val cdf1 = search_sorted(Data1, data_all).map(i => i.toDouble / n1)
    val cdf2 = search_sorted(Data2, data_all).map(i => i.toDouble / n2)
    val statistic = max(abs(cdf1.zip(cdf2).map{case (a,b) => a-b}))
    //val en = sqrt(n1 * n2 / (n1 + n2).toDouble)

    val ks_distribution = new KolmogorovSmirnovTest()
    var prob = 0.0
    try {
      prob = ks_distribution.approximateP(statistic, n1, n2)
    }
    catch {
      case _ => prob = 1.0
    }
    new KSTwoSampleTestResult(statistic, prob, "Two samples are statistically the same distribution.")
  }
//RDD[data1] & RDD[data2] must be sorted, and zipped by index, [Double, col, index]
  //should be cached
  /*
  Computes the Kolmogorov-Smirnov statistic on 2 samples.

    This is a two-sided test for the null hypothesis that 2 independent samples
    are drawn from the same continuous distribution.

    Parameters
    ----------
    a, b : sequence of 1-D ndarrays
        two arrays of sample observations assumed to be drawn from a continuous
        distribution, sample sizes can be different

    Returns
    -------
    D : float
        KS statistic
    p-value : float
        two-tailed p-value

    Notes
    -----
    This tests whether 2 samples are drawn from the same distribution. Note
    that, like in the case of the one-sample K-S test, the distribution is
    assumed to be continuous.

    This is the two-sided test, one-sided tests are not implemented.
    The test uses the two-sided asymptotic Kolmogorov-Smirnov distribution.

    If the K-S statistic is small or the p-value is high, then we cannot
    reject the hypothesis that the distributions of the two samples
    are the same.

    Examples
    --------
    >>> from scipy import stats
    >>> np.random.seed(12345678)  #fix random seed to get the same result
    >>> n1 = 200  # size of first sample
    >>> n2 = 300  # size of second sample

    For a different distribution, we can reject the null hypothesis since the
    pvalue is below 1%:

    >>> rvs1 = stats.norm.rvs(size=n1, loc=0., scale=1)
    >>> rvs2 = stats.norm.rvs(size=n2, loc=0.5, scale=1.5)
    >>> stats.ks_2samp(rvs1, rvs2)
    (0.20833333333333337, 4.6674975515806989e-005)

    For a slightly different distribution, we cannot reject the null hypothesis
    at a 10% or lower alpha since the p-value at 0.144 is higher than 10%

    >>> rvs3 = stats.norm.rvs(size=n2, loc=0.01, scale=1.0)
    >>> stats.ks_2samp(rvs1, rvs3)
    (0.10333333333333333, 0.14498781825751686)

    For an identical distribution, we cannot reject the null hypothesis since
    the p-value is high, 41%:

    >>> rvs4 = stats.norm.rvs(size=n2, loc=0.0, scale=1.0)
    >>> stats.ks_2samp(rvs1, rvs4)
    (0.07999999999999996, 0.41126949729859719)
   */
  def ks_2samp_sc(data1:RDD[(Double,Int, Long)], data2:RDD[(Double,Int, Long)]): KSTwoSampleTestResult = {
    val data_merge = data1 ++ data2
    val n1 = data1.count().toInt
    val n2 = data2.count().toInt
    val col1 = data1.first._2
    //val col2 = data2.first._2
    val br_n1 = data1.sparkContext.broadcast(n1)
    val br_n2 = data1.sparkContext.broadcast(n2)
    val br_col1 = data1.sparkContext.broadcast(col1)
   // val br_col2 = data1.sparkContext.broadcast(col2)

    val statistic = data_merge.sortBy(i => i._1).zipWithIndex().map{case ((value, col, index_self), index_join) => {
      var n:Long = 0
      if(col == br_col1.value) {
        n = br_n1.value
      }
      else n = br_n2.value
      abs((index_self + 1).toDouble / n - (index_join - index_self ).toDouble / n)
      }
    }.max()
    val ks_distribution = new KolmogorovSmirnovTest()
    var prob = 0.0
    try {
      prob = ks_distribution.exactP(statistic, n1, n2, true)
    }
    catch {
      case _ => prob = 1.0
    }
    new KSTwoSampleTestResult(statistic, prob, "Two samples are statistically the same distribution.")

  }
}