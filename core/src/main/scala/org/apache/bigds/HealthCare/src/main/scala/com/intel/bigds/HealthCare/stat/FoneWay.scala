package com.intel.bigds.HealthCare.stat

//import breeze.stats.distributions.FDistribution

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkException, Logging}
import org.apache.commons.math3.distribution.FDistribution
import org.apache.spark.mllib.rdd.RDDFunctions._


object FoneWay extends Logging {

  def SumofSquare(input:Array[Double]):Double = {
    val Squared = for (i <- input ) yield {
      i*i
    }
    Squared.sum
  }

  def SquareofSum(input:Array[Double]):Double = {
    val Sum = input.sum
    Sum*Sum
  }

  //local computing of FoneWayTest
  def FoneWayTest(observed: Array[Array[Double]]):FoneWayTestResult = {
    val na = observed.length
    val alldata = observed.flatMap(i => i)
    val bign = alldata.length
    val sstot = SumofSquare(alldata) - SquareofSum(alldata) / bign.toDouble
    var ssbn = 0.0
    for (a <- observed) {
      ssbn += SquareofSum(a) / a.length.toDouble
    }
    ssbn -= (SquareofSum(alldata)) / bign.toDouble
    val sswn = sstot - ssbn
    val dfbn = na - 1
    val dfwn = bign - na
    val msb = ssbn / dfbn.toDouble
    val msw = sswn / dfwn.toDouble
    val f = msb / msw
    val Fdistribution = new FDistribution(dfbn.toDouble, dfwn.toDouble)
    val prob = 1 - Fdistribution.cumulativeProbability(f)
    new FoneWayTestResult(f, prob, dfbn, dfwn, "All observations of the factor produce the same response")

  }

  /**
   * Performs a 1-way ANOVA.

   * The one-way ANOVA tests the null hypothesis that two or more groups have
   * the same population mean.  The test is applied to samples from two or
   * more groups, possibly with differing sizes.
   * @param input
   * @return
   */

  //distributed computing of FoneWayTest
  //method "raw" means ignore all the losted data, F one way test is only applied to existing data (namely, each column may have different lengths)
  //method "filled" means each column has the same length. But the result depends on the data cleaning method
  def FoneWayTest(input: RDD[Array[String]],attribute_num:Int, ColLength:Array[Long]/*, method:String = "raw"*/):FoneWayTestResult = {
    val observed = input.map(i => i.map(_.toDouble))//observed: RDD[Array[Double]]
    val sc = observed.sparkContext
    //val SumPerCol:Array[(Double, Double)], length = observed.first.length
    val zero = new Array[(Double,Double)](attribute_num).map{i => (0.0,0.0)}
    val SumPerCol = observed.treeAggregate(zero)(
      seqOp = (U, r) => {
        U.zip(r).map{case ((a,b),c) => (a + c, b + c * c)}
      },
      combOp = (U1, U2) => {
        U1.zip(U2).map{case ((a,b),(c,d)) => (a + c, b + d)}
      }
    )
    val alldata_square_sum = SumPerCol.map(_._2).sum //sums of square, ss(alldata)
    val alldata_sum = SumPerCol.map(_._1).sum
    val alldata_sum_square = alldata_sum * alldata_sum //square_of_sums(alldata)
    val feature_len = observed.count
    val bign = feature_len * attribute_num

    val sstot = alldata_square_sum - alldata_sum_square / bign.toDouble
    val ssbn = SumPerCol.map(_._1).zipWithIndex.map{case (sumcol, index) => {
     /* if (method == "raw") {
        sumcol * sumcol / input.ColLength(index)
      }
      else {
        sumcol * sumcol / input.ColFullLength
      }*/
      sumcol * sumcol / ColLength(index)
    }
    }.sum - alldata_sum_square / bign.toDouble
    val sswn = sstot - ssbn
    val dfbn = attribute_num - 1
    val dfwn = bign - attribute_num
    val msb = ssbn / dfbn.toDouble
    val msw = sswn / dfwn.toDouble
    val f = msb / msw
    val Fdistribution = new FDistribution(dfbn.toDouble, dfwn.toDouble)
    val prob = 1 - Fdistribution.cumulativeProbability(f)
    new FoneWayTestResult(f, prob, dfbn, dfwn.toInt, "All observations of the factor produce the same response")

  }


}