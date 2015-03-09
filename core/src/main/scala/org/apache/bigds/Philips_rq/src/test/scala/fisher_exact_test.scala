package org.com.intel.Philips.stat

import breeze.linalg.{DenseMatrix=>BDM}
import com.intel.Philips.stat.FiExact
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import scala.util.Random

object FiExactTestSuite {
  def generateFiExactTestInput(sc: SparkContext, nPart: Int = 4, nPoints: Int, seed:Int, nFeatures: Int, methodName: String = "2x2"): RDD[LabeledPoint] = {
    val rnd = new Random(seed)
    val ran_gen = Range(0, nPoints).map(i => (rnd.nextInt(2), rnd.nextInt))
    val ran_gen2 = ran_gen.map{ case (a,b) => LabeledPoint(a,
      Vectors.dense((for (i<- (0 until nFeatures)) yield {
            val rnd_sub = new Random(b)
            rnd_sub.nextInt(2).toDouble
          }).toArray))
        }
    val gen_data = sc.parallelize(ran_gen2, nPart)
    gen_data
  }
}

class FiExactTestSuite extends FunSuite {

  test("Fisher's exact test") {
    val data = Array[Int](8,1,2,5)
    val contingency = new BDM(2, 2, data)
    val result = FiExact.FiExactMatrix(contingency)

    assert(result.pValue==0.034964034965034919)
  }
}



