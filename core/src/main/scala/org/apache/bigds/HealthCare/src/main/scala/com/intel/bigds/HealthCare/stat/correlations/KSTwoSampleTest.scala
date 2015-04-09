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
    val prob = ksTestModel.exactP(statistic, data1.length, data2.length, true)
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
      prob = ks_distribution.exactP(statistic, n1, n2, true)
    }
    catch {
      case _ => prob = 1.0
    }

    new KSTwoSampleTestResult(statistic, prob, "Two samples are statistically the same distribution.")
  }
//RDD[data1] & RDD[data2] must be sorted, and zipped by index, [Double, col, index]
  //should be cached
  def ks_2samp_sc(data1:RDD[(Double,Int, Int)], data2:RDD[(Double,Int, Int)]): KSTwoSampleTestResult = {
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