package sjtu.spark.example

import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable

/**
 * Created by Ou Huisi on 15/1/19.
 */

object testTensor {

  case class Params(
     input: String = null,
     kryo: Boolean = false,
     numIterations: Int = 4,
     lambda: Double = 1.0,
     rank: Int = 10,
     dim: Int = 4,
     nsize: Array[Int] = Array(11,12,13,14),
     numBlocks: Array[Int] = Array(-1,-1,-1,-1),
     implicitPrefs: Boolean = false) extends AbstractParams[Params]

  def main (args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("TensorALS") {
      head("TensorALS: an example app for ALS on Tensor data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("dim")
        .text(s"dim, default: ${defaultParams.dim}}")
        .action((x, c) => c.copy(dim = x))

      //TODO more than one parameter, need to update scopt
//      opt[Seq[Int]]("size").valueName("<size1>,<size2>...")
//        .text(s"size, default: ${defaultParams.nsize}}")
//        .action{(x, c) => c.copy(jars = x)}

//      opt[Int]("numBlocks")
//        .text(s"number of blocks, default: ${defaultParams.numBlocks} (auto)")
//        .action((x, c) => c.copy(numBlocks = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text("use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.MovieLensALS \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --dim 2 --rank 5 --numIterations 20 --lambda 1.0 --kryo \
          |  data/mllib/sample_movielens_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"TensorALS with $params").setMaster("local")
    if (params.kryo) {
      conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[SparseTensor]))
        .set("spark.kryoserializer.buffer.mb", "8")
    }
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val implicitPrefs = params.implicitPrefs

    val dim = params.dim
    var nsize = params.nsize

    val sparseTensor = sc.textFile(params.input).map { line =>
      val fields = line.split("::")
      if (implicitPrefs) {
        /*
         * MovieLens sparseTensor are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the predicted rating is less than 3.
         * To map sparseTensor to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */

        //TODO implicit
        val nsub = new Array[Int](dim)
        for (i <- 0 until dim) {
          nsub(i) = fields(i).toInt
        }
        new SparseTensor(nsize, nsub, fields(dim).toDouble)

      } else {
        val nsub = new Array[Int](dim)
        for (i <- 0 until dim) {
          nsub(i) = fields(i).toInt
        }
        //create a sparsetensor for each line
        new SparseTensor(nsize, nsub, fields(dim).toDouble)
      }
    }.cache()

    //RDD operation is lazy, count can cause it implement
//    val numRatings = sparseTensor.count()
//    println(s"Got $numRatings SparseTensor.")

    //split the sparsetensors into two parts: train and test
    val splits = sparseTensor.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()

    val test = if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */

      //TODO implicit
      splits(1)
    } else {
      splits(1)
    }.cache()

    //if not necessary, don't do it
//    val numTraining = training.count()
//    val numTest = test.count()
//    println(s"Training: $numTraining, test: $numTest.")

    sparseTensor.unpersist(blocking = false)

    val factors = new TensorALS()
      .setDim(params.dim)
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setBlocks(params.numBlocks)
      .setSize(params.nsize)
      .run(training)

    //test for the matrix factorization
//    val rmse = TensorUtils.computeMatrixRmse(factors, test, params.implicitPrefs)
//    println(s"Test RMSE = $rmse.")
    sc.stop()
  }

}
