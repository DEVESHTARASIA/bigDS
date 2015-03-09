package com.intel.Philips.stat

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.stat.Statistics._

object ChisqwithData {
  def main(args:Array[String]): Unit ={
    if (args.length != 2){
      System.err.println("parameter required: <spark master address> <file address> ")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("Chisquare test with Philips Data")

    val sc = new SparkContext(conf)

    val file_address = args(1)
    val data = sc.textFile(file_address).map(i => i.split(",")).zipWithIndex().filter(_._2 > 0).map(_._1)

    //In this case, Chi-square two-sample test is conducted on column(1,7), (2, 7), (3, 7), (5, 7)
    val test_data = DataContainer.To_LabeledData(data, Array(1,2,3,5), 7)

    println("=============printed test data============")
    test_data.foreach{case LabeledPoint(label, features) => println(label + " || " + features.toArray.mkString(" ")) }
    val independenceResult = chiSqTest(test_data)

    println("=============test result=============")
    independenceResult.foreach(i => {
      println(i.toString())
    })

  }
}