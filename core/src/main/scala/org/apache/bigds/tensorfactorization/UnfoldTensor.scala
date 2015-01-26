package main.scala.sjtu.spark.example

import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Ou Huisi on 15/1/20.
 */

/**
 * An unfolded sparse tensor.

    Data is stored in form of a sparse COO matrix.
    Unfolded_sptensor objects additionall hold information about the
    original tensor, such that re-folding the tensor into its original
    shape can be done easily.

    Unfolded_sptensor objects can be instantiated via

 * @param x : coordinate x
 * @param y : coordinate y
 * @param xsize : shape of x
 * @param ysize : shape of y
 * @param rdim : ArrayBuffer of integers
 *             modes of the original tensor that are mapped onto rows.
 * @param cdim : ArrayBuffer of integers
 *             modes of the original tensor that are mapped onto columns.
 * @param v : value of the unfoldtensor
 *
 */
class UnfoldTensor (
      val x: Int,
      val y: Int,
      val xsize: Int,
      val ysize: Int,
      val rdim: ArrayBuffer[Int],
      val cdim: ArrayBuffer[Int],
      val v: Double) extends Serializable with Logging {

}