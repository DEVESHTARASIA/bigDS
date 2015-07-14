package org.apache.spark.armlib.examples

import org.apache.spark.armlib.fpm.PFIN
import org.apache.spark.{SparkContext, SparkConf}

import scala.compat.Platform._

/**
 * Created by clin3 on 2015/4/26.
 */
object example {
  def main(args: Array[String]): Unit = {
    val input = "hdfs://10.1.2.71:54311/user/clin/fpgrowth/input/" + args(0)
    val minSupport = args(1).toDouble
    val numPartition = args(2).toInt
    val conf = new SparkConf()
      .setAppName("App")
      .set("spark.cores.max", "224")
      .set("spark.executor.memory", "160G")
      .setMaster("spark://sr471:7180")

    val sc = new SparkContext(conf)

    val startTime = currentTime
    val transactions = sc.textFile(input, numPartition).map(_.split(" ")).cache()

    val model = new PFIN()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)
/*    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)*/
    val numFreqItemsets = model.freqItemsets.count()
    val endTime = currentTime
    val totalTime: Double = endTime - startTime

    val numTransactions = transactions.count()

/*    for(i <- model.freqItemsets.collect()) {
      for(j <- i._1) {
        print(j + " ")
      }
      print("\r\n")
      print(" support = " + i._2 + "\r\n")
    }*/
    println(s"============ PFINv2a - STATS ==============")
    println(s" minSupport = " + minSupport + s"    numPartition = " + numPartition)
    println(s" Number of transactions: " + numTransactions)
    println(s" Number of frequent itemsets: " + numFreqItemsets)
    println(s" Total time = " + totalTime/1000 + "s")
    println(s"========================================")

    sc.stop()
  }
}
