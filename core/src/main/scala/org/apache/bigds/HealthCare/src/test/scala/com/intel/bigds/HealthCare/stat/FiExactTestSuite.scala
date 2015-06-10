package com.intel.bigds.HealthCare.stat

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import com.intel.bigds.HealthCare.preprocessing._
import org.apache.spark.rdd.RDD

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.TestingUtils._

import scala.util.Random

/*
class FiExactTestSuite extends FunSuite with MLlibTestSparkContext {
  def generateFiExactTestInput(sc: SparkContext, nPart: Int = 4, nPoints: Int, seed:Int, nFeatures: Int, methodName: String = "2x2"): RDD[LabeledPoint] = {
    val rnd = new Random(seed)
    val ran_gen = Range(0, nPoints).map(i => (rnd.nextInt(2), rnd.nextInt))
    val ran_gen2 = ran_gen.map{ case (a,b) => {
      val rnd_sub = new Random(b)
      val features = for (i<-(0 until nFeatures)) yield {
        rnd_sub.nextInt(2).toDouble
      }
      LabeledPoint(a, Vectors.dense(features.toArray))
    }
    }
    val gen_data = sc.parallelize(ran_gen2, nPart)
    gen_data
  }
  test("testFiExact") {
    val gen_data = generateFiExactTestInput(sc, 4, 20, 26, 10, "2x2")
    println("=======================test data begin=========================")
    gen_data.collect.map(a=>a.toString()).foreach(println)
    println("=======================test data end=========================")

    val result = FiExact.FiExactFeatures(gen_data)

    for (i <- (0 until result.length)) {
      val pValue = result(i).pValue
      val oddsratio = result(i).oddsRatio
      println(s"result for feature $i is pValue: $pValue, oddsratio: $oddsratio")
    }
    assert(true)
  }
}
*/