package com.intel.bigds.stat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.util.Random

object FiExactTest {

  //generate test data for Fisher's exact test. The data format per-transaction is : LabeledPoint(1, Vector(0,1,1,0,0,1))
  //nPoints: number of transactions; nFeatures: number of features to be tested. Unit test is conducted between Label & one of the features
  //Only 2 categories for labels and features are supported currently. So method name is "2x2"
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

  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      System.err.println("parameter required: <spark master address>")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster(args(0))
      .setAppName("fisher's exact")
    val sc = new SparkContext(conf)

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

    sc.stop()

    /*
     val data = Array[Int](9,2,1,4)
     val contingency = new BDM(2, 2, data)
     val result = FiExact.FiExactMatrix(contingency)
     println(result.pValue)
     println(result.oddsRatio)*/
  }
}