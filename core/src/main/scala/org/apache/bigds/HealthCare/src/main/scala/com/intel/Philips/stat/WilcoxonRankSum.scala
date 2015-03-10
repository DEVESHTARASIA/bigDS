package com.intel.bigds.stat

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Random

object WilcoxonRankSum {
  def main(args: Array[String]) {
    println("Wilcoxon Rank Sum")

    if (args.length != 1){
      System.err.println("parameter required: <spark master address>")
      System.exit(1)
    }

    val conf = new SparkConf()
                 .setMaster(args(0))
                 .setAppName("Wilcox")

    val sc = new SparkContext(conf)

    //Generate one Gaussian distributed data set (small set, local) and one normal distributed data set (large, distributed) to do Wilcox test
    val rnd = new Random(26)
    val length = 20
    val nparts = 4
    val set_small_length = 10
    val small_set_seed = Range(0, set_small_length).map(i => rnd.nextInt)
    val small_set = small_set_seed.map{i =>
      val rnd_sub = new Random(i)
      val data = for (i<-0 until length) yield {
        rnd_sub.nextGaussian().abs
      }
      data.toArray
    }
    val large_set_seed = Range(0, nparts).map(i => (i*25, (i+1)*25, rnd.nextInt))
    val large_set = sc.parallelize(large_set_seed, nparts).flatMap{i =>
      var seed = i._3
      val sub_data = for (j <- 0 until 25) yield {
        val rnd_sub = new Random(seed)
        val data = for (i <- 0 until length) yield {
          rnd_sub.nextFloat().toDouble
        }
        seed = rnd_sub.nextInt()
        data.toArray
      }
      sub_data
    }
    println("=============data set 1============")
    small_set.map(i=>i.mkString(",")).foreach(println)
    println("=============data set 2============")
    large_set.collect.map(i=>i.mkString(",")).foreach(println)

    val result = WilcoxRankSumTest.pWilcoxTest(sc, small_set.toArray, large_set)
    println(result.count)

    println("=============Wilcox result=============")
    result.collect.map(i=>i.mkString(",")).foreach(println)

    sc.stop()
  }


}

