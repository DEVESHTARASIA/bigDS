package sjtu.spark.example

import scala.math.{abs, sqrt}
import scala.util.Random
import scala.util.hashing.byteswap32

import org.jblas.{DoubleMatrix, Solve}

import org.apache.spark.{HashPartitioner, Logging, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by Ou Huisi on 15/1/22.
 */

class TensorALS private (
      private var rank: Int,
      private var iterations: Int,
      private var lambda: Double,
      private var implicitPrefs: Boolean,
      private var alpha: Double,
      private var dim: Int,
      private var numBlocks: Array[Int],
      private var size: Array[Int],
      private var seed: Long = System.nanoTime()
      ) extends Serializable with Logging {

  /**
   * Constructs an TensorALS instance with default parameters: {numBlocks: (-1,-1,-1,-1), rank: 10, iterations: 10,
   * lambda: 0.01, implicitPrefs: false, alpha: 1.0, size: (10,12,11,8), dim: 4}.
   */
  def this() = this(10, 10, 0.01, false, 1.0, 4, Array(-1,-1,-1,-1), Array(10,12,11,8))

  //========TODO block ALS==========

  /** If true, do alternating nonnegative least squares. */
  private var nonnegative = false

  /** storage level for user/product in/out links */
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  //================================

  /** Set the dim of the ratings (number of dimension). Default: 4. */
  def setDim(dim: Int): this.type = {
    this.dim = dim
    this
  }

  /** Set the blocks of the tensor. Default: (-1,-1,-1,-1). */
  def setBlocks(b: Array[Int]): this.type = {
    this.numBlocks = b
    this
  }

  /** Set the rank of the feature matrices computed (number of features). Default: 10. */
  def setRank(rank: Int): this.type = {
    this.rank = rank
    this
  }

  /** Set the number of iterations to run. Default: 10. */
  def setIterations(iterations: Int): this.type = {
    this.iterations = iterations
    this
  }

  /** Set the size of the tensor. */
  def setSize(size: Array[Int]): this.type = {
    this.size = size
    this
  }

  /** Set the regularization parameter, lambda. Default: 0.01. */
  def setLambda(lambda: Double): this.type = {
    this.lambda = lambda
    this
  }

  /** Sets whether to use implicit preference. Default: false. */
  def setImplicitPrefs(implicitPrefs: Boolean): this.type = {
    this.implicitPrefs = implicitPrefs
    this
  }

  /**
   * :: Experimental ::
   * Sets the constant used in computing confidence in implicit TensorALS. Default: 1.0.
   */
  @Experimental
  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  /** Sets a random seed to have deterministic results. */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Set whether the least-squares problems solved at each iteration should have
   * nonnegativity constraints.
   */
  def setNonnegative(b: Boolean): this.type = {
    this.nonnegative = b
    this
  }

  /**
   * :: DeveloperApi ::
   * Sets storage level for intermediate RDDs (user/product in/out links). The default value is
   * `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g., `MEMORY_AND_DISK_SER` and
   * set `spark.rdd.compress` to `true` to reduce the space requirement, at the cost of speed.
   */
  @DeveloperApi
  def setIntermediateRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    this.intermediateRDDStorageLevel = storageLevel
    this
  }

  /**
   * Run TensorALS with the configured parameters on an input RDD of SparseTensor.
   * Returns feature factors for each item.
   */
  def run(ratings: RDD[SparseTensor]) : Array[DoubleMatrix] = {

    val sc = ratings.context

    //set numBlocks
    val numBlocks = new Array[Int](dim)
    for (i <- 0 until dim) {
      numBlocks(i) = if (this.numBlocks(i) == -1) {
        math.max(sc.defaultParallelism, ratings.partitions.size / 2)
      } else {
        this.numBlocks(i)
      }
    }

    //create tensor partitioner for each domain
    val partitioners = new Array[TensorPartitioner](dim)
    for (i <- 0 until dim){
      partitioners(i) = new TensorPartitioner(numBlocks(i))
    }

    //separate tensor by each block
    val ratingsByBlock = new Array[RDD[(Int, SparseTensor)]](dim)
    for (i <- 0 until dim) {
      ratingsByBlock(i) = ratings.map(rating => (partitioners(i).getPartition(rating.sub(i)), rating))
    }

    //group tensors by each block id
    val grouped = new Array[RDD[(Int, SparseTensor)]](dim)
    for (i <- 0 until dim) {
      grouped(i) = ratingsByBlock(i).partitionBy(new HashPartitioner(numBlocks(i)))
    }

    //initialize factors
    val U = new Array[DoubleMatrix](dim)
    for (i <- 0 until dim) {
      U(i) = DoubleMatrix.rand(size(i), rank)
    }

    //TODO calculate fit
//    val fit = 0

    //calculate factors for each iteration
    for (itr <- 0 until iterations) {
      println(s"Iteration: $itr")
//      val fitold = fit

      //calculate each factor
      for (n <- 0 until dim) {
        //return a list of factor:
        //for each tensor(rating), calculate Unfolded tensor times Khatri-Rao product
        //id is the unfold dimension (n), each rating returns a vector
        //sum the vactor by id (the sub of dimension(n)), collect a list of id and vector
        val Usep = grouped(n).map(x => (x._2.sub(n),x._2.uttkrp(U,n))).reduceByKey(_.add(_)).collect()

        //reform the list of factor to be a DoubleMatrix (id is the row)
        val Ugrouped = DoubleMatrix.zeros(size(n),rank)
        for (i <- 0 until Usep.length) {
          Ugrouped.putRow(Usep(i)._1, Usep(i)._2)
        }

        //calculate Y, if new factor is A, then Y = B'B * C'C *...
        var Y = DoubleMatrix.ones(rank, rank)
        for (i <- 0 until dim) {
          if(i != n) {
            Y = Y.mul(U(i).transpose().mmul(U(i)))
          }
        }

        //calculate pinv(Y)
        //should be set a tmp val to store the value or else it will crush  TODO can still crush!
        val pinvY = Solve.pinv(Y)
//        val Unew = Solve.solvePositive(Y, Ugrouped)  //This way doesn't work!

        //scale factors if needed
//        var lmbda = new DoubleMatrix()
//        if(itr == 0) {
//          lmbda = MatrixFunctions.sqrt(Unew.mul(Unew).columnSums())
//        }
//        else {
//          lmbda = Unew.columnMaxs()
//          for (j <- 0 until Unew.columns) {
//            if (lmbda.get(0, j) < 1) {
//              lmbda.put(0, j, 1)
//            }
//          }
//        }
//        U(n) = Unew.divRowVector(lmbda)

        //calculate new factor
        U(n) = Ugrouped.mmul(pinvY)

        println(s"factor $n finished.")
      }
    }

    U
  }

  //TODO will be used in block ALS
  /**
   * Make a random factor vector with the given random.
   */
  private def randomFactor(rank: Int, rand: Random): Array[Double] = {
    // Choose a unit vector uniformly at random from the unit sphere, but from the
    // "first quadrant" where all elements are nonnegative. This can be done by choosing
    // elements distributed as Normal(0,1) and taking the absolute value, and then normalizing.
    // This appears to create factorizations that have a slightly better reconstruction
    // (<1%) compared picking elements uniformly at random in [0,1].
    val factor = Array.fill(rank)(abs(rand.nextGaussian()))
    val norm = sqrt(factor.map(x => x * x).sum)
    factor.map(x => x / norm)
  }

}


/**
 * Partitioner for ALS.
 */
class TensorPartitioner(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    TensorUtils.nonNegativeMod(byteswap32(key.asInstanceOf[Int]), numPartitions)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case p: TensorPartitioner =>
        this.numPartitions == p.numPartitions
      case _ =>
        false
    }
  }
}
