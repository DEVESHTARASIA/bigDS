package org.apache.bigds.tensorfactorization

import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Ou Huisi on 15/1/23.
 */

/**
 * Some basic function
 */
object TensorUtils {

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  def nonNegativeMod (x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  /**
   * check the multiplication dims and indices
   */
  def checkMultiplicationDims (mode: Int, N: Int, M: Int, vflag: Boolean=false, without: Boolean=false): (Array[Int], Array[Int]) = {
    val dims = new ArrayBuffer[Int]
    for(i <- 0 until N) {
      if(i!=mode) {
        dims.append(i)
      }
    }
    val P = dims.length
    val (sdims, sidx) = argSort(dims.toArray)

    var vidx = new Array[Int](P)
    if (vflag) {
      if (P == M) {
        vidx = sidx
      }
      else {
        vidx = sdims
      }
      (sdims, vidx)
    }
    else {
      (sdims, Array(0))
    }
  }

  /**
    Returns the indices that would sort an array.

    Perform an indirect sort along the given axis using the algorithm specified
    by the `kind` keyword. It returns an array of indices of the same shape as
    `a` that index data along the given axis in sorted order.

    Parameters
    ----------
    oArray : array_like
        Array to sort.

    Returns
    -------
    (oArray, idx) :
        Sorted oArray and
        Array of indices that sort origin `oArray` along the specified axis.
        In other words, origin ``oArray[idx]`` yields a sorted `oArray` (which is returned too).

   */
  def argSort (oArray: Array[Int]): (Array[Int], Array[Int]) = {

    val idx = new Array[Int](oArray.length)
    for (i <- 0 until idx.length) {
      idx(i) = i 
    }

    for (i <- oArray.length-1 to 0 by -1) {
      for (j <- 0 until i) {
        if(oArray(j) > oArray(j+1)) {
          val tmp1 = oArray(j)
          oArray(j) = oArray(j+1)
          oArray(j+1) = tmp1
          val tmp2 = idx(j)
          idx(j) = idx(j+1)
          idx(j+1) = tmp2
        }
      }
    }
    (oArray, idx)
  }

  /**
   *Helper function to create ranges with missing entries
   */
  def fromToWithout (frm: Int, stop: Int, without: Int, step: Int=1, skip: Int=1): ArrayBuffer[Int] = {
    val a = new ArrayBuffer[Int]
    for (i <- frm until without by step) {
      a.append(i)
    }
    for (i <- without+skip until stop by step) {
      a.append(i)
    }

    a
  }

//  /** Compute RMSE of matrix (Root Mean Squared Error). */
//  def computeMatrixRmse (factors: Array[DoubleMatrix], data: RDD[SparseTensor], implicitPrefs: Boolean): Double = {
//
//    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
//
//    val predict = factors(0).mmul(factors(1).transpose())
//    val predictionsAndRatings = data.map(x => Math.pow((predict.get(x.sub(0), x.sub(1)) - x.v),2)).collect()
//    var sum = 0.0
//    for (i <- 0 until predictionsAndRatings.length) {
//      sum += predictionsAndRatings(i)
//    }
//    sum /= predictionsAndRatings.length
//    Math.sqrt(sum)
//  }

  /** Compute RMSE of tensor (Root Mean Squared Error). */
  def computeRmse (factors: Array[DoubleMatrix], data: RDD[SparseTensor], implicitPrefs: Boolean): Double = {

    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r

    val R = factors(0).getColumns
    val loss = data.map { tmpTensor =>
      var preVector = DoubleMatrix.ones(R)
      for (n <- 0 until factors.length) {
        preVector = preVector.mul(factors(n).getRow(tmpTensor.sub(n)))
      }
      Math.pow((preVector.sum() - tmpTensor.v), 2)
    }.collect()

    var sum = 0.0
    for (i <- 0 until loss.length) {
      sum += loss(i)
    }
    sum /= loss.length
    Math.sqrt(sum)
  }
}
