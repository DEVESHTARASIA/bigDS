package org.apache.spark.mllib.linalg.distributed

import com.github.fommil.netlib.BLAS.{ getInstance => blas }
import org.apache.spark.mllib.stat.{PatchedMultivariateOnlineSummarizer, MultivariateOnlineSummarizer, MultivariateStatisticalSummary}

// import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{ Matrices, Vector, Matrix }
import org.apache.spark.mllib.rdd.RDDFunctions._
import breeze.linalg.{ Vector => BV, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV }
import org.apache.spark.SparkContext
import breeze.numerics.sqrt

import breeze.linalg.{ svd => brzSvd, axpy => brzAxpy }
import breeze.numerics.{ sqrt => brzSqrt }

import java.util.Arrays

/**
 * Created by lv on 14-7-30.
 * Modified by Chunnan on 15-5-15
 * computeColumnSummaryStatistics is added. Useful in missing data filling and binning.
 */
class PatchedRowMatrix(@transient val sc: SparkContext, val nparts: Int, override val rows: RDD[Vector],
                       private var nRows: Long,
                       private var nCols: Int) extends RowMatrix(rows, nRows, nCols) {

  def this(sc: SparkContext, nparts: Int, rows: RDD[Vector]) = this(sc, nparts, rows, 0L, 0)

  //val m = numRows.toInt
  val n = numCols.toInt

  val blocks = rows.zipWithIndex
    .glom
    .map { vecs =>

    val mb = vecs.length
    val nb = n

    var start = -1

    val block = new Array[Double](mb * nb)
    vecs.foreach {
      case (vec, i) =>
        if (start < 0) start = i.toInt
        vec.toArray.copyToArray(block, (i.toInt - start) * nb, nb)
    }
    (start, mb, nb, block)
  }.cache
/*
  val blocks_svd = rows.zipWithIndex
    .glom
    .map { vecs =>
    val mb = vecs.length
    val nb = n
    var start = -1
    val block = new Array[Double](mb * nb)
    vecs.foreach {
      case (vec, i) =>
        if (start < 0) start = i.toInt
        vec.toArray.copyToArray(block, (i.toInt - start) * nb, nb)
    }
    (start, mb, nb, block)
  }*/


  def multiplyBlockBy(v: Array[Double]): Array[Double] = {

    val vbr = sc.broadcast(v)
    blocks.mapPartitions { iters => {
      val vbr_value = vbr.value
      iters.flatMap { case (start, mb, nb, arr) =>
        val total = new Array[Double](mb)
        // blas.dgemv("N", mb, nb, 1, arr, mb, vbr.value, 1, 0, total, 1)

        var offset = 0
        for (i <- 0 until mb) {
          for (j <- 0 until nb) {
            total(i) += arr(offset) * vbr_value(j)
            offset += 1
          }
        }
        total
      }
    }
    }
      .collect
  }

  def multiplyTBlockBy(v: Array[Double]): Array[Double] = {

    val vbr = sc.broadcast(v)

    blocks.map {
      case (start, mb, nb, arr) =>
        val piece = vbr.value.slice(start, start + mb)
        val total = new Array[Double](nb)
        // blas.dgemv("T", mb, nb, 1, arr, mb, piece, 1, 0, total, 1)

        var offset = 0
        for (i <- 0 until mb) {
          for (j <- 0 until nb) {
            total(j) += arr(offset) * piece(i)
            offset += 1
          }
        }

        total
    }.reduce((r1, r2) => r1.zip(r2).map { case (e1, e2) => e1 + e2 })
    /*
    .treeAggregate(new Array[Double](nCols))(
      seqOp = (U, r) => {
        r
      }, combOp = (U1, U2) => U1.zip(U2).map{ case(u1, u2) => u1 + u2 }
    )
     */
  }

  /** Updates or verifies the number of rows. */
  private def updateNumRows(m: Long) {
    if (nRows <= 0) {
      nRows = m
    } else {
      require(nRows == m,
        s"The number of rows $m is different from what specified or previously computed: ${nRows}.")
    }
  }

  override def computeColumnSummaryStatistics(): MultivariateStatisticalSummary = {
    val summary = rows.treeAggregate(new PatchedMultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
    updateNumRows(summary.count)
    summary
  }
}

  /*
    // compute covariance matrix with result as a distributed matrix, i.e., RDD[(Int, Array[Double])]
    def computeDistributedCovariance(): RDD[(Int, Array[Double])] = {
      val n = numCols().toInt
      val m = numRows().toInt
      val (m, mean) = rows.treeAggregate[(Long, BDV[Double])]((0L, BDV.zeros[Double](n)))(
        seqOp = (s: (Long, BDV[Double]), v: Vector) => (s._1 + 1L, s._2 += v.toBreeze),
        combOp = (s1: (Long, BDV[Double]), s2: (Long, BDV[Double])) =>
          (s1._1 + s2._1, s1._2 += s2._2))
      if (m <= 1) {
        sys.error(s"PatchedRowMatrix.computeDistributedCovariance called on matrix with only $m rows." +
          "  Cannot compute the covariance of a RowMatrix with <= 1 row.")
      }
      updateNumRows(m)
      mean :/= m.toDouble
      // We use the formula Cov(X, Y) = E[X * Y] - E[X] E[Y], which is not accurate if E[X * Y] is
      // large but Cov(X, Y) is small, but it is good for sparse computation.
      // TODO: find a fast and stable way for sparse data.
      val m1 = m - 1.0
      var alpha = 0.0
      // Transpose, convert from n(cols) * m(rows) => m(cols) * n(rows)
      var rows_T: Array[Double] = new Array[Double](n * m)
      val rows_collect = rows.collect()
      for (i <- 0 until m) {
        for (j <- 0 until n) {
          rows_T(j * m + i) = rows_collect(i).toArray(j)
        }
      }
      val shared_rows = sc.broadcast(rows_T)
      val triangle_index = sc.parallelize(0 until ((n * n + n) / 2), nparts)
      triangle_index.flatMap { index =>
        def get_x_y_from_tri_index(index: Int, n: Int): (Int, Int) = {
          // calculate x,y position in 2d matrix according to triangle index
          // ex: for 4 x 4 triangle (n=4)
          // 0 1 2 3
          //   4 5 6
          //     7 8
          //       9
          def tri_height_to_index(x: Int) = ((2L * n * x - x.toLong * x + x) / 2L).toInt
          def index_to_tri_height(sum: Int) = (((2L * n + 1) - sqrt((2L * n + 1L) * (2L * n + 1L) - 8L * sum)) / 2L).toInt
          val height = index_to_tri_height(index)
          val offset = index - tri_height_to_index(height)
          (height, height + offset)
        }
        val M = shared_rows.value
        val (i, j) = get_x_y_from_tri_index(index, n)
        val v = blas.ddot(m, M, j * m, 1, M, i * m, 1)
        Seq((i, (j, v)), (j, (i, v)))
      }
        .groupByKey(nparts)
        .map {
          case (i, iter) =>
            alpha = m / m1 * mean(i)
            val arr = new Array[Double](n)
            iter.map {
              case (j, v) =>
                arr(j) = v / m1 - alpha * mean(j)
            }
            (i, arr)
        }
    }
     // compute gramian matrix with result as a distributed matrix, i.e., RDD[(Int, Array[Double])]
    def computeDistributedGramianMatrix(): RDD[(Int, Array[Double])] = {
      val n = numCols().toInt
      val m = numRows().toInt
      // Transpose, convert from n(cols) * m(rows) => m(cols) * n(rows)
      var rows_T: Array[Double] = new Array[Double](n * m)
      val rows_collect = rows.collect()
      for (i <- 0 until m) {
        for (j <- 0 until n) {
          rows_T(j * m + i) = rows_collect(i).toArray(j)
        }
      }
      val shared_rows = sc.broadcast(rows_T)
      val triangle_index = sc.parallelize(0 until ((n * n + n) / 2), nparts)
      triangle_index.mapPartitions { iters =>
        def get_x_y_from_tri_index(index: Int, n: Int): (Int, Int) = {
          // calculate x,y position in 2d matrix according to triangle index
          // ex: for 4 x 4 triangle (n=4)
          // 0 1 2 3
          //   4 5 6
          //     7 8
          //       9
          def tri_height_to_index(x: Int) = ((2L * n * x - x.toLong * x + x) / 2L).toInt
          def index_to_tri_height(sum: Int) = (((2L * n + 1) - sqrt((2L * n + 1L) * (2L * n + 1L) - 8L * sum)) / 2L).toInt
          val height = index_to_tri_height(index)
          val offset = index - tri_height_to_index(height)
          (height, height + offset)
        }
        val M = shared_rows.value
        iters.flatMap{ index =>
        val (i, j) = get_x_y_from_tri_index(index, n)
        val v = blas.ddot(m, M, j * m, 1, M, i * m, 1)
        Seq((i, (j, v)), (j, (i, v)))
      }
      }
        .groupByKey(nparts)
        .map {
          case (i, iter) =>
            val arr = new Array[Double](n)
            iter.map {
              case (j, v) =>
                arr(j) = v
            }
            (i, arr)
        }
    }*/
/*
  // compute covariance matrix with result as a distributed matrix, i.e., RDD[(Int, Array[Double])]
  def computeDistributedCovariance(): RDD[(Int, Array[Double])] = {
    val n = numCols().toInt
    val (m, mean) = rows.treeAggregate[(Long, BDV[Double])]((0L, BDV.zeros[Double](n)))(
      seqOp = (s: (Long, BDV[Double]), v: Vector) => (s._1 + 1L, s._2 += v.toBreeze),
      combOp = (s1: (Long, BDV[Double]), s2: (Long, BDV[Double])) =>
        (s1._1 + s2._1, s1._2 += s2._2))
    if (m <= 1) {
      sys.error(s"PatchedRowMatrix.computeDistributedCovariance called on matrix with only $m rows." +
        "  Cannot compute the covariance of a RowMatrix with <= 1 row.")
    }
    updateNumRows(m)
    mean :/= m.toDouble
    // We use the formula Cov(X, Y) = E[X * Y] - E[X] E[Y], which is not accurate if E[X * Y] is
    // large but Cov(X, Y) is small, but it is good for sparse computation.
    // TODO: find a fast and stable way for sparse data.
    val m1 = m - 1.0
    var alpha = 0.0
    val m2 = numRows().toInt
    // Transpose, convert from n(cols) * m(rows) => m(cols) * n(rows)
    // var rows_T: Array[Double] = new Array[Double](n * m2)
    val rows_T = Array.ofDim[Double](n, m2)
    val rows_collect = rows.collect()
    for (i <- 0 until m2) {
      for (j <- 0 until n) {
        // rows_T(j * m2 + i) = rows_collect(i).toArray(j)
        rows_T(j)(i) = rows_collect(i).toArray(j)
      }
    }
    val shared_rows = sc.broadcast(rows_T)
    val triangle_index = sc.parallelize(0 until ((n * n + n) / 2), nparts)
    triangle_index.flatMap { index =>
      def get_x_y_from_tri_index(index: Int, n: Int): (Int, Int) = {
        // calculate x,y position in 2d matrix according to triangle index
        // ex: for 4 x 4 triangle (n=4)
        // 0 1 2 3
        //   4 5 6
        //     7 8
        //       9
        def tri_height_to_index(x: Int) = ((2L * n * x - x.toLong * x + x) / 2L).toInt
        def index_to_tri_height(sum: Int) = (((2L * n + 1) - sqrt((2L * n + 1L) * (2L * n + 1L) - 8L * sum)) / 2L).toInt
        val height = index_to_tri_height(index)
        val offset = index - tri_height_to_index(height)
        (height, height + offset)
      }
      val M = shared_rows.value
      val (i, j) = get_x_y_from_tri_index(index, n)
      // val v = blas.ddot(m2, M, j * m2, 1, M, i * m2, 1)
      val v = blas.ddot(m2, M(j), 1, M(i), 1)
      Seq((i, (j, v)), (j, (i, v)))
    }
      .groupByKey(nparts)
      .map {
      case (i, iter) =>
        alpha = m / m1 * mean(i)
        val arr = new Array[Double](n)
        iter.map {
          case (j, v) =>
            arr(j) = v / m1 - alpha * mean(j)
        }
        (i, arr)
    }
  }*/

  /*
  override def multiplyGramianMatrixBy(v: BDV[Double]): BDV[Double] = {
    val vbr = sc.broadcast(v.toArray)
    val n = numCols().toInt
    val result = blocks_svd.mapPartitions { iters => {
      val vbr_value = vbr.value
      iters.map { case (start, mb, nb, arr) =>
        // val blas_result1 = new Array[Double](mb)
        // blas.dgemv("N", mb, nb, 1, arr, mb, vbr.value, 1, 0, blas_result1, 1)
        val piece = new Array[Double](mb)
        var offset = 0
        for (i <- 0 until mb) {
          for (j <- 0 until nb) {
            piece(i) += arr(offset) * vbr_value(j)
            offset += 1
          }
        }
        // if (piece.zipWithIndex.filter { case (v, i) => math.abs(v - blas_result1(i)) > 0.1 }.length > 0) println("Yes, we catch a big error!!!")
        /*
        val blas_result2 = new Array[Double](nb)
        blas.dgemv("T", mb, nb, 1, arr, mb, blas_result1, 1, 0, blas_result2, 1)
         */
        val total = new Array[Double](nb)
        offset = 0
        for (i <- 0 until mb) {
          for (j <- 0 until nb) {
            total(j) += arr(offset) * piece(i)
            offset += 1
          }
        }

        // if (total.zipWithIndex.filter{ case(v, i) => math.abs(v - blas_result2(i)) > 0.001 }.length > 0) println("Yes, we catch a big error!!!")

        total
      }
    }
    }
      .treeReduce((r1, r2) => {
      r2.zipWithIndex.map { case (e, i) => r1(i) += e }
      r1
    })
    /*
    .treeAggregate(new Array[Do(uble]n))(
      seqOp = (U, r) => {
        r
      }, combOp = (U1, U2) => U1.zip(U2).map{ case(u1, u2) => u1 + u2 }
    )
     */
    BDV(result)
  }*/


/*
  def multiplyTBlockBy(v: Array[Double]): Array[Double] = {
    val vbr = sc.broadcast(v)
    blocks.map {
      case (start, mb, nb, arr) =>
        val piece = vbr.value.slice(start, start + mb)
        val total = new Array[Double](nb)
        // blas.dgemv("T", mb, nb, 1, arr, mb, piece, 1, 0, total, 1)
        var offset = 0
        for (i <- 0 until mb) {
          for (j <- 0 until nb) {
            total(j) += arr(offset) * piece(i)
            offset += 1
          }
        }
        total
    }.reduce((r1, r2) => r1.zip(r2).map { case (e1, e2) => e1 + e2 })
    /*
    .treeAggregate(new Array[Double](nCols))(
      seqOp = (U, r) => {
        r
      }, combOp = (U1, U2) => U1.zip(U2).map{ case(u1, u2) => u1 + u2 }
    )
     */
  }
  */



  /* override def computeSVD(
     // private def computeMySVD(
     k: Int,
     computeU: Boolean,
     rCond: Double,
     maxIter: Int,
     tol: Double,
     mode: String): SingularValueDecomposition[RowMatrix, Matrix] = {
     val n = numCols().toInt
     require(k > 0 && k <= n, s"Request up to n singular values but got k=$k and n=$n.")
     object SVDMode extends Enumeration {
       val LocalARPACK, LocalLAPACK, DistARPACK = Value
     }
     val computeMode = mode match {
       case "auto" =>
         // TODO: The conditions below are not fully tested.
         if (n < 100 || k > n / 2) {
           // If n is small or k is large compared with n, we better compute the Gramian matrix first
           // and then compute its eigenvalues locally, instead of making multiple passes.
           if (k < n / 3) {
             SVDMode.LocalARPACK
           } else {
             SVDMode.LocalLAPACK
           }
         } else {
           // If k is small compared with n, we use ARPACK with distributed multiplication.
           SVDMode.DistARPACK
         }
       case "local-svd" => SVDMode.LocalLAPACK
       case "local-eigs" => SVDMode.LocalARPACK
       case "dist-eigs" => SVDMode.DistARPACK
       case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
     }
     // Compute the eigen-decomposition of A' * A.
     val (sigmaSquares: BDV[Double], u: BDM[Double]) = computeMode match {
       case SVDMode.LocalARPACK =>
         require(k < n, s"k must be smaller than n in local-eigs mode but got k=$k and n=$n.")
         val G = this.computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]
         EigenValueDecomposition.symmetricEigs(v => G * v, n, k, tol, maxIter)
       case SVDMode.LocalLAPACK =>
         val G = this.computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]
         val brzSvd.SVD(uFull: BDM[Double], sigmaSquaresFull: BDV[Double], _) = brzSvd(G)
         (sigmaSquaresFull, uFull)
       case SVDMode.DistARPACK =>
         require(k < n, s"k must be smaller than n in dist-eigs mode but got k=$k and n=$n.")
         // val gram = computeDistributedGramianMatrix()
         // EigenValueDecomposition.symmetricEigs(muiltiplyGramianBy, n, k, tol, maxIter)
         EigenValueDecomposition.symmetricEigs(multiplyGramianMatrixBy, n, k, tol, maxIter)
       // NewEigenValueDecomposition.symmetricEigs(rows, n, k, tol, maxIter)
       // NewEigenValueDecomposition.symmetricEigs(multiplyGramianMatrixBy, n, k, tol, maxIter)
     }
     val sigmas: BDV[Double] = brzSqrt(sigmaSquares)
     // Determine the effective rank.
     val sigma0 = sigmas(0)
     val threshold = rCond * sigma0
     var i = 0
     // sigmas might have a length smaller than k, if some Ritz values do not satisfy the convergence
     // criterion specified by tol after max number of iterations.
     // Thus use i < min(k, sigmas.length) instead of i < k.
     if (sigmas.length < k) {
       logWarning(s"Requested $k singular values but only found ${sigmas.length} converged.")
     }
     while (i < math.min(k, sigmas.length) && sigmas(i) >= threshold) {
       i += 1
     }
     val sk = i
     if (sk < k) {
       logWarning(s"Requested $k singular values but only found $sk nonzeros.")
     }
     val s = Vectors.dense(Arrays.copyOfRange(sigmas.data, 0, sk))
     val V = Matrices.dense(n, sk, Arrays.copyOfRange(u.data, 0, n * sk))
     if (computeU) {
       // N = Vk * Sk^{-1}
       val N = new BDM[Double](n, sk, Arrays.copyOfRange(u.data, 0, n * sk))
       var i = 0
       var j = 0
       while (j < sk) {
         i = 0
         val sigma = sigmas(j)
         while (i < n) {
           N(i, j) /= sigma
           i += 1
         }
         j += 1
       }
       val U = this.multiply(Matrices.fromBreeze(N))
       SingularValueDecomposition(U, s, V)
     } else {
       SingularValueDecomposition(null, s, V)
     }
   }*/
/*
  def computeFullGramianMatrix(): Matrix = {
    // override def computeGramianMatrix(): Matrix = {
    val n = numCols().toInt
    val m = numRows().toInt
    println(s"in patched gramian matrix, $n x $m")
    // Compute the upper triangular part of the gram matrix.
    // Transpose, convert from n(cols) * m(rows) => m(cols) * n(rows)
    var rows_T: Array[Double] = new Array[Double](n * m)
    val rows_collect = rows.collect()
    for (i <- 0 until m) {
      for (j <- 0 until n) {
        rows_T(j * m + i) = rows_collect(i).toArray(j)
      }
    }
    val shared_rows = sc.broadcast(rows_T)
    val avg = n / nparts
    val ind = n - avg * nparts
    val G = new Array[Double](n * n)
    val result = sc.parallelize(0 until nparts, nparts)
      .mapPartitions { iters =>
      val shared_value = shared_rows.value
      iters.map{ipart =>
        val offset = (if (ipart < ind) ipart * avg + ipart else ipart * avg + ind)
        val size = (if (ipart < ind) avg + 1 else avg)
        val arr = new Array[Double](size * n)
        // var cur = 0
        // val M = shared_rows.value
        // for(i <- 0 until size) {
        //   for(j <- 0 until n) {
        //     arr(cur) = blas.ddot(m, M, (offset+i)*m, 1, M, j*m, 1)
        //     cur += 1
        //   }
        // }
        val B =shared_value
        val A = B.slice(offset * m, (offset + size) * m)
        blas.dgemm("N", "T", size, n, m, 1, A, size, B, n, 0, arr, size)
        (offset, arr)
      }}
      .collect
      .map {
      case (offset, arr) =>
        arr.copyToArray(G, offset * n)
    }
    shared_rows.unpersist()
    Matrices.dense(n, n, G)
  }
  /* override def computeGramianMatrix(): Matrix = {
     // def computeTriangleGramianMatrix(): Matrix = {
     val n = numCols().toInt
     val m = numRows().toInt
     println(s"in patched gramian matrix, $n x $m")
     // Compute the upper triangular part of the gram matrix.
     // Transpose, convert from n(cols) * m(rows) => m(cols) * n(rows)
     var rows_T: Array[Double] = new Array[Double](n * m)
     val rows_collect = rows.collect()
     for (i <- 0 until m) {
       for (j <- 0 until n) {
         rows_T(j * m + i) = rows_collect(i).toArray(j)
       }
     }
     // val covA = new Array[Double](n * n)
     // for (i <- 0 until n) {
     //   for (j <- 0 until n) {
     //     covA(i*n+j) = blas.ddot(m, rows_T, j*m, 1, rows_T, i*m, 1)
     //   }
     // }
     val shared_rows = sc.broadcast(rows_T)
     val triangle_index = sc.parallelize(0 until ((n * n + n) / 2), nparts)
     val result = triangle_index.mapPartitions(iters => {
     def get_x_y_from_tri_index(index: Int, n: Int): (Int, Int) = {
       // calculate x,y position in 2d matrix according to triangle index
       // ex: for 4 x 4 triangle (n=4)
       // 0 1 2 3
       //   4 5 6
       //     7 8
       //       9
       def tri_height_to_index(x: Int) = ((2L * n * x - x.toLong * x + x) / 2L).toInt
       def index_to_tri_height(sum: Int) = (((2L * n + 1) - sqrt((2L * n + 1L) * (2L * n + 1L) - 8L * sum)) / 2L).toInt
       val height = index_to_tri_height(index)
       val offset = index - tri_height_to_index(height)
       (height, offset)
       // (height + offset, height)
     }
     val M = shared_rows.value
       iters.map { index =>
         val (i, j) = get_x_y_from_tri_index(index, n)
         // for (k <- 0 until m) {
         //   v += M(j * m + k) * M(i * m + k)
         // }
         val v = blas.ddot(m, M, (i + j) * m, 1, M, i * m, 1)
         // println(s"$index, $i, $j => $v")
         (i, (j, v))
       }
     })
     val G = new Array[Double](n * n)
     result.groupByKey(nparts)
       .map {
         case (i, iter) =>
           val arr = new Array[Double](n - i)
           iter.map {
             case (j, v) =>
               arr(j) = v
           }
           arr
       }
       .collect
       .foreach { arr =>
         var i = n - arr.length
         var j = i * n + i
         arr.copyToArray(G, i * n + i)
         var k = 1
         while (k < arr.length) {
           j += n
           G(j) = arr(k)
           k += 1
         }
       }
     // var noequal = 0
     // for(i <- 0 until n*n) {
     //   if (G(i)!=covA(i)) noequal += 1
     // }
     // println("--------------- <NOEQUAL == " + noequal + "> --------------------")
     // if(covA.equals(G)) {
     //   println("--------------------- YES, EQUALS -----------------------")
     // } else {
     //   println("--------------------- NO, NOT EQUALS -----------------------")
     // }
     shared_rows.unpersist()
     Matrices.dense(n, n, G)
   }*/

  /**
   * private[mllib] def generateOmega (n : Int, r : Int, seed : Int) : Array[Double] = {
   *
   * val rand = new Random(seed)
   *
   * val omega = new Array[Double](n*r)
   * for(i <- 0 until omega.length) {
   * omega(i) = rand.nextDouble * 2.0 - 1.0
   * }
   * }
   *
   * def computeStochasticSVD(
   * k: Int,
   * p: Int,
   * computeU: Boolean,
   * rCond: Double,
   * maxIter: Int,
   * tol: Double,
   * mode: String) : SingularValueDecomposition[RowMatrix, Matrix] = {
   *
   * // stochastic singular value decomposition algorithm
   * val m = numRows().toInt
   * val n = numCols().toInt
   * val minmn = m min n
   *
   * require(k > 0 && k <= minmn, s"Request up to min(m,n) singular values but got k=$k and m=$m n=$n.")
   *
   * val r = if ((k + p) < minmn) k + p else r
   *
   * val seed = new Random().nextInt
   * val brOmega = sc.broadcast(generateOmega(n, r, seed))
   *
   * rows.mapPartitionsWithIndex{ case(ind, iter) =>
   * val block = iter.map{ arr =>
   * val B = shared_rows.value
   * val A = B.slice(offset * m, (offset + size) * m)
   * blas.dgemm("N", "N", size, n, m, 1.0, A, size, B, m, 0.0, arr, size)
   * (offset, arr)
   * }
   * }
   *
   * rows
   *
   * }
   */
}
*/
