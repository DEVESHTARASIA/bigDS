package com.intel.bigds.HealthCare.stat

import org.apache.spark.rdd.RDD
import Alternative._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import scala.util.Sorting.quickSort
import scala.math.min
import breeze.numerics.sqrt
import org.apache.commons.math3.special.Erf
import org.apache.commons.math3.stat.inference.MannWhitneyUTest


/**
 * Created by lv on 14-5-29.
 * ported from wilcox.c & choose.c in R
 */


object WilcoxRankSumTest extends Serializable{
  val MAX_M=30
  var w: Array[Array[Array[Double]]] = Array.fill[Double](MAX_M, MAX_M, MAX_M)(-1)
//  var allocated_m = 0L
//  var allocated_n = 0L
//}

  val sqrt2 = sqrt(2.0)
  val gMu = 0
  val gSigma = 1
  val gfactor = sqrt2 * gSigma

//class WilcoxRankSumTest extends Serializable{
//  implicit def Int2Double(v:RDD[Int]) = v.foreach{x=>x.toDouble}
//  implicit def Long2Double(v:RDD[Long]) = v.foreach{x=>x.toDouble}

  def math_compute(x: Array[Double], y:Array[Double]): Double = {
    val test = new MannWhitneyUTest()
    test.mannWhitneyUTest(x, y)
  }

  def pnorm(z:Double, left_tail:Boolean=true):Double = {

    val v = 0.5 * (1 + Erf.erf( (z - gMu)/ gfactor))

    if (!left_tail) 1-v else v
  }

  def pwilcox(q:Long, m:Long, n:Long, left_tail:Boolean=true):Double = {
    if (m+n<MAX_M) pwilcox_small(q, m, n, left_tail) else pwilcox_large(q,m,n, left_tail)
  }


  def wilcox_rank_sum_Spark(x:RDD[Double], y:RDD[Double], alternative:Alternative.Value = TwoSided):WilcoxRankSumTestResult = {
    val threshold = x.count()
    val z = (x ++ y).zipWithIndex().sortByKey() // join two sequence together and sort by value
    //    println("z:", z.collect().toList)
    val z2 = z.zipWithIndex().map({ case ((v, p), r) => (v, p, r + 1)}) // add rank position according to sorted sequence
    //    println("z2:", z2.collect().toList)
    val z3 = z2.groupBy({ case (v, p, r) => v}) // groupby value
    //    println("z3:", z3.collect().toList)


    val accum = z3.map((i) => {
      val rank_val_sum = i._2.foldLeft(0L) { case (a, (v, p, r)) => a + r}
      val rank_count = i._2.count({ case ((v, p, r)) => true})
      //      println("iter:", i, i._2, rank_val_sum, rank_count)
      (rank_val_sum.toDouble / rank_count) * i._2.count({ case (v, p, r) => p >= threshold}) //select x
    }).reduce({ case (a, b) => a + b})
    //accum - threshold*(threshold+1).toDouble/2
    val w = accum
    val m = x.count
    val n = y.count
    if ((m == 0) || (n == 0)) return new WilcoxRankSumTestResult(w, 0.0, "Two Attributes are statistically same")
    val pValue = alternative match {
      case TwoSided => min(2 * (if (w > (m * n / 2)) pwilcox(w.toLong - 1, m, n, left_tail = false) else pwilcox(w.toLong, m, n)), 1.0)
      case Less => pwilcox(w.toLong, m, n)
      case Greater => pwilcox(w.toLong - 1, m, n, left_tail = false)
    }
    new WilcoxRankSumTestResult(w, pValue, "Two Attributes are statistically same")
  }

  def wilcox_rank_sum(x:Array[Double], y:Array[Double], alternative:Alternative.Value = TwoSided): WilcoxRankSumTestResult = {
    val threshold = x.length
    val z = (x ++ y).toList.zipWithIndex.sortBy{case (v, k)=>v}                  // join two sequence together and sort by value

    //    println("z:", z.collect().toList)
    val z2 = z.zipWithIndex.map({case ((v,p),r) => (v, p, r+1)})  // add rank position according to sorted sequence
    //    println("z2:", z2.collect().toList)
    val z3 = z2.groupBy({case (v,p,r) => v})     // groupby value
    //    println("z3:", z3.collect().toList)

    //var w:Double = 0

    val accum = z3.map((i)=>{
      val rank_val_sum = i._2.foldLeft(0L){case (a, (v,p,r)) => a+r}
      val rank_count   = i._2.count({case((v,p,r))=>true})  //count with condition
      //      println("iter:", i, i._2, rank_val_sum, rank_count)
      val group_avg = (rank_val_sum.toDouble / rank_count) * i._2.count({ case (v, p, r) => p >= threshold})
      //val group_avg = (rank_val_sum.toDouble / rank_count)
     // val y_sum = i._2.filter( _._1._2 ==0 ).length * group_avg
      group_avg
    }).sum
    //accum - threshold*(threshold+1).toDouble/2
    val w = accum
    val m = x.length
    val n = y.length
    if ((m==0)||(n==0)) return new WilcoxRankSumTestResult(w, 0.0, "Two Attributes are statistically same")
    val pValue = alternative match {
      case TwoSided => min(2 * (if (w > (m * n / 2)) pwilcox(w.toLong - 1, m, n, left_tail = false) else pwilcox(w.toLong, m, n)), 1.0)
      case Less => pwilcox(w.toLong, m, n)
      case Greater => pwilcox(w.toLong - 1, m, n, left_tail = false)
    }
    new WilcoxRankSumTestResult(w, pValue, "Two Attributes are statistically same")
  }


  /*def pWilcoxTest_Spark(x:RDD[Double], y:RDD[Double], alternative:Alternative.Value = TwoSided):Double={
    val m = x.count()
    val n = y.count()
    val w = W_sc(x, y)

//    println("W:", w)
    alternative match {
      case TwoSided => min(2* (if (w > (m * n /2)) pwilcox(w.toLong-1, m, n, left_tail = false) else pwilcox(w.toLong, m, n)), 1.0)
      case Less => pwilcox(w.toLong, m, n)
      case Greater => pwilcox(w.toLong-1, m, n, left_tail = false)
    }
  }*/

  /*def pWilcoxTest(sc:SparkContext, x:Array[Array[Double]], y:RDD[Array[Double]], alternative:Alternative.Value = TwoSided):RDD[Array[Double]] = {
    val brx = sc.broadcast(x)
    val pvals = y.map { i =>
      brx.value.map { j =>
        val m = j.length
        val n = i.length
        //val w = W(j, i)
        val w = W(j, i)
        if ((m == 0) || (n == 0))  0.0 // FIXME: will need Wilcoxon signed rank test
        //    println("W:", w)
        alternative match {
          case TwoSided => min(2 * (if (w > (m * n / 2)) pwilcox(w.toLong - 1, m, n, left_tail = false) else pwilcox(w.toLong, m, n)), 1.0)
          case Less => pwilcox(w.toLong, m, n)
          case Greater => pwilcox(w.toLong - 1, m, n, left_tail = false)
        }
      }
    }
    pvals
  }*/


 /* def pWilcoxTest_unit(m: Int, n: Int, w: Double, alternative:Alternative.Value = TwoSided):Double = {

    if ((m==0)||(n==0)) return 0.0 // FIXME: will need Wilcoxon signed rank test
    //    println("W:", w)
    alternative match {
      case TwoSided => min(2* (if (w > (m * n /2)) pwilcox(w.toLong-1, m, n, left_tail = false) else pwilcox(w.toLong, m, n)), 1.0)
      case Less => pwilcox(w.toLong, m, n)
      case Greater => pwilcox(w.toLong-1, m, n, left_tail = false)
    }
  }*/
  

  def pwilcox_large(q:Long, m:Long, n:Long, left_tail:Boolean=true): Double = {
    var z:Double = q - m*n.toDouble/2
    val sigma:Double = sqrt((m*n.toDouble / 12) * (n+m+1))
    z = (z-1)/sigma
    pnorm(z, left_tail)
  }

  private def choose(n:Long, k:Long):Double = {  // Binomial coefficient
    var c:Double = 1.0
    var i:Int = 1
    for (i <- 1L to k) {
      c = c * (n - (k - i)).toDouble / i
    }
    c
  }

  def pwilcox_small(q:Long, m:Long, n:Long, left_tail:Boolean=false): Double = {
    var i:Int=0
    var c:Double=0
    var p:Double=0
    var lower_tail = left_tail

    if (m <= 0 || n <= 0) throw new Exception("NAN")
    if ((m > MAX_M) || (n> MAX_M)) throw new Exception("m or n is too large for pwilcox_small")

    if (q < 0)
        return 0
    if (q >= m * n)
        return 1

//    wInitMayBe(m, n)
    c = choose(m + n, n)
    p = 0
    /* Use summation of probs over the shorter range */
    if (q <= (m * n / 2)) {
      for (i <-  0L to q)
        p += cwilcox(i, m, n) / c
    } else {
      var qq = m * n - q
      for (i <- 0L to qq-1)
        p += cwilcox(i, m, n) / c
        lower_tail = !lower_tail  /* p = 1 - p; */
    }
    if (lower_tail) p else 1-p
  }

  private def cwilcox(k:Long, m:Long, n:Long):Double = {
    var u = m * n

    if (k < 0 || k > u)
      return 0
    var c = u / 2
    var i:Int = 0
    var j:Int = 0
    var kk = k
    if (kk > c) {
      kk = u - kk
    } /* hence  kk <= floor(u / 2) */
    if (m < n) {
        i = m.toInt
        j = n.toInt
    } else {
        i = n.toInt
        j = m.toInt
    } /* hence  i <= j */

    if (j == 0) /* and hence i == 0 */
        return if (kk == 0) 1 else 0

    /* We can simplify things if k is small.  Consider the Mann-Whitney
       definition, and sort y.  Then if the statistic is k, no more
       than k of the y's can be <= any x[i], and since they are sorted
       these can only be in the first k.  So the count is the same as
       if there were just k y's.
    */
    if (j > 0 && kk < j) return cwilcox(kk, i, kk)
    val w = WilcoxRankSumTest.w
//    w .synchronized {
//      if (w(i)(j) == null) {
//        w(i)(j) = new Array[Double](c.toInt + 1)
//
//        for (l <- 0L to c) {
//          w(i)(j)(l.toInt) = -1
//        }
//      }
//    }
    if (w(i)(j)(kk.toInt) < 0) {
        if (j == 0) /* and hence i == 0 */
            w(i)(j)(kk.toInt) = if (kk == 0) 1 else 0
        else
            w(i)(j)(kk.toInt) = cwilcox(kk - j, i - 1, j) + cwilcox(kk, i, j - 1)

    }
    w(i)(j)(kk.toInt)
  }

//  private def wInitMayBe(m:Long, n:Long) = this.synchronized{
//    var mm=m
//    var nn=n
//    var i= nn
//
//    if (m > n) {
//      nn = mm
//      mm = i
//    }
//    if ((WilcoxRankSumTest.w != null) && (m > WilcoxRankSumTest.allocated_m || n > WilcoxRankSumTest.allocated_n))
//        WilcoxRankSumTest.w.synchronized {
//          wFree(WilcoxRankSumTest.allocated_m, WilcoxRankSumTest.allocated_n); /* zeroes w */
//        }
//
//    if (WilcoxRankSumTest.w == null) { /* initialize w[][] */
//        WilcoxRankSumTest.w = new Array[Array[Array[Double]]](mm.toInt+1)
//        for (i <- 0L to mm) {
//            WilcoxRankSumTest.w(i.toInt) = new Array[Array[Double]](nn.toInt+1)
//        }
//        WilcoxRankSumTest.allocated_m = mm; WilcoxRankSumTest.allocated_n = nn
//    }
//  }
//
//  private def wFree(m: Long, n: Long) {
//    var i=m
//    var j=n
//    for (i <- m to 0 by -1) {
//      for (j <-  n to 0 by -1) {
//          if (WilcoxRankSumTest.w(i.toInt)(j.toInt) != null)
//              WilcoxRankSumTest.w(i.toInt)(j.toInt) = null
//      }
//      WilcoxRankSumTest.w(i.toInt) = null
//    }
//    WilcoxRankSumTest.w = null
//    WilcoxRankSumTest.allocated_m = 0
//    WilcoxRankSumTest.allocated_n = 0
//  }
}
