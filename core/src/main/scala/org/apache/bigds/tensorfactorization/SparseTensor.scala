package sjtu.spark.example

import main.scala.sjtu.spark.example.UnfoldTensor
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Logging
import org.jblas.{DoubleMatrix}

/**
 * Created by Ou Huisi on 15/1/16.
 */

/**
 * A sparse tensor.
 * Data is stored in COOrdinate format.
 * Sparse tensors can be instantiated via
 *
 * @param tshape : shape : n-tuple, optional
 *               Shape of the sparse tensor.
 *               Length of tuple n must be equal to dimension of tensor.
 * @param sub : n-tuple of array-likes
 *            Subscripts of the nonzero entries in the tensor.
 *            Length of tuple n must be equal to dimension of tensor.
 * @param v : array-like
 *            Values of the nonzero entries in the tensor.
 */
class SparseTensor(
      val tshape: Array[Int],
      val sub: Array[Int],
      val v: Double) extends Serializable with Logging with Ratings {

  def this() = this(Array(30,100), Array(1,2), 2)
  val ndim = sub.length

  /**
   * Unfolding Tensor for dimension rdim for further usage
   *
   * @param rdim : ArrayBuffer to save unfolding dimension
   * @return : UnfoldTensor
   */
  def unfold (rdim: ArrayBuffer[Int]): UnfoldTensor = {
    val cdim = new ArrayBuffer[Int]
    for (i <- 0 until ndim) {
      rdim.foreach(x => (
        if(i != x) {
        cdim.append(i)
      }))
    }

    var M = 1
    rdim.foreach(M *= tshape(_))
    val ridx = this.buildIndex(rdim)

    var N = 1
    cdim.foreach(N *= tshape(_))
    val cidx = this.buildIndex(cdim)

    new UnfoldTensor(ridx, cidx, M, N, rdim, cdim, v)
  }

  /**
   * build the index when unfold the tensor
   */
  def buildIndex (dim: ArrayBuffer[Int]): Int = {
    val rshape = new ArrayBuffer[Int]
    dim.foreach(x => (rshape.append(tshape(x))))

    val rsub = new ArrayBuffer[Int]
    dim.foreach(x => (rsub.append(sub(x))))

    var idx = 0
    for(i <- rsub.length-1 to 0 by -1) {
      var weight = 1
      for (j<- 0 until i) {
        weight *= rshape(j)
      }
      idx += rsub(i) * weight
    }

    idx
  }

  /**
   * Tensor times vector product
   *
   * @param v : Array of DoubleMatrix Vector to be multiplied with tensor.
   * @param dims : dimension that is reduced
   * @param vidx : the column index of the vector which to be calculate
   * @param remdims : the remain dimension that is not reduced
   * @return : SparseTensor
   */
  def ttvCompute (v: ArrayBuffer[DoubleMatrix], dims: Array[Int], vidx: Array[Int], remdims: ArrayBuffer[Int]): SparseTensor = {
    var nvals = this.v
    val nsubs = this.sub
    for (i <- 0 until dims.length) {
      val idx = nsubs(dims(i))
      val w = v(vidx(i))
      nvals *= w.get(idx)
    }

    //TODO consider special case to ensure the function can be used commonly
//    if (remdims.length == 0) {
//      nvals
//    }

    val ttvsub = new Array[Int](remdims.length)
    val ttvshape = new Array[Int](remdims.length)
    for (i <- 0 until remdims.length) {
      ttvsub(i) = this.sub(remdims(i))
      ttvshape(i) = this.tshape(remdims(i))
    }

    new SparseTensor(ttvshape, ttvsub, nvals)
  }

  /**
   *  Tensor times vector product interface
   *
   *  @param v : Array of DoubleMatrix Vector to be multiplied with tensor.
   *  @param modes : array_like of integer
   *  @param without : boolean, optional
   *                 If True, vectors are multiplied in all modes
   *                 **except** the modes specified in ``modes``.
   *  @return : SparseTensor
   */
  def ttv (v: ArrayBuffer[DoubleMatrix], modes: Int, without: Boolean = false): SparseTensor = {
    val (dims, vidx) = TensorUtils.checkMultiplicationDims(modes, this.ndim, v.length, true, without)
    val remdims = new ArrayBuffer[Int]
    val subTest = new Array[Boolean](ndim)
    for (i <- 0 until dims.length) {
      subTest(dims(i)) = true
    }

    for (i <- 0 until this.ndim) {
      if (subTest(i) == false) {
        remdims.append(i)
      }
    }

    ttvCompute(v, dims, vidx, remdims)
  }

  /**
   * Unfolded tensor times Khatri-Rao product:
   * math:`M = \\unfold{X}{3} (U_1 \kr \cdots \kr U_N)`
   * Computes the _matrix_ product of the unfolding
   * of a tensor and the Khatri-Rao product of multiple matrices.
   * Efficient computations are perfomed by the respective tensor implementations.
   *
   * @param U : list of array-likes
   *          Matrices for which the Khatri-Rao product is computed and
   *          which are multiplied with the tensor in mode ``mode``.
   * @param mode: int
   *            Mode in which the Khatri-Rao product of ``U`` is multiplied
   *            with the tensor.
   * @return : DoubleMatrix
   *         Matrix which is the result of the matrix product of the unfolding of
   *         the tensor and the Khatri-Rao product of ``U``
   * See also
   * References
   * ----------
   * .. [1] B.W. Bader, T.G. Kolda
   * Efficient Matlab Computations With Sparse and Factored Tensors
   * SIAM J. Sci. Comput, Vol 30, No. 1, pp. 205--231, 2007
   */
  def uttkrp (U: Array[DoubleMatrix], mode: Int): DoubleMatrix = {
    val R = if (mode == 0) U(1).getColumns else U(0).getColumns
    val dims = TensorUtils.fromToWithout(0, this.ndim, mode)

    val V = DoubleMatrix.zeros(R)

    for (r <- 0 until R) {
      val Z = new ArrayBuffer[DoubleMatrix]
      dims.foreach(x => Z.append(U(x).getColumn(r)))
      val TZ = ttv(Z, mode, true)
      V.put(r, TZ.v)
    }

    V
  }
}
