package org.apache.bigds.mining.association.HybridFP

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.compat.Platform._

/**
 * Created by clin3 on 2014/12/23.
 */

object HybridFPGrowthTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("spark://sr471:7177")
      .setAppName("HybridFPGrowth")
      .set("spark.cores.max", "256")
      .set("spark.executor.memory", "160G")

    val sc = new SparkContext(conf)

    val supportThreshold = args(0).toDouble
    val fileName = args(1)
//    val splitterPattern = args(2)

    val startTime = currentTime
    val data = sc.textFile("hdfs://sr471:54311/user/clin/fpgrowth/input/" + fileName)
    val frequentItemsets = HybridFPGrowth.run(data, supportThreshold)
    val count = frequentItemsets.count
    val endTime = currentTime
    val totalTime: Double = endTime - startTime

    println("---------------------------------------------------------")
    println("This program totally took " + totalTime/1000 + " seconds.")
    println("Number of frequent itemsets using HybridFPGrowth= " + count)
    println("---------------------------------------------------------")

    println("---------------------------------------------------------")
    println("Frequent Itemsets")
    frequentItemsets.collect.foreach { case (itemsets, cnt) =>
      println("<" + itemsets + ", " + cnt + ">")
    }
  }
}
