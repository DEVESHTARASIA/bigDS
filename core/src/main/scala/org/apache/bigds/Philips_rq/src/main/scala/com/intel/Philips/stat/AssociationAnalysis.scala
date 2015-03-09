package com.intel.Philips.stat

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.fpm


object AssociationAnalysis {

  //transform categorical data (normal/abnormal; yes/no) of each item genre (A1, A2, ...) into (A1=normal, A2 = no, A3 = abnormal, ....)
  def FpmParse(data_body: RDD[Array[String]], item_name: Array[String], item_list: Array[Int]): RDD[Array[String]] = {
//item_name: the name of each column; item_list: the position of choosed item column; data_body: data to be processed
    val after_data = data_body.map(i => {
      for (j <- item_list) {
        i(j) = item_name(j) + "=" + i(j)
      }
      i
    })
    after_data.map(i => {
      for (j <- item_list) yield {
        i(j)
      }
    })
  }

  //do the test on Philips' synthesized data, choose item genre (1,2,3,4,5,6,7,8,9,10) in the data
  def main(args: Array[String]): Unit = {
    println("Association rule analysis")
    if (args.length != 2){
      System.err.println("parameter required: <spark master> <file address> ")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("association rule analysis")

    val sc = new SparkContext(conf)

    val file_address = args(1)

    val row_data = sc.textFile(file_address).map(i => i.split(",")).cache()

    val item_name = row_data.first()
    val data_body = row_data.zipWithIndex().filter(_._2 > 0).map(_._1)
    val data_body_container = new DataContainer(data_body)
    val na = Set("?")
    val item_list = Array(1,2,3,4,5,6,7,8,9,10)

    val data_body_fill = data_body_container.Cleaning(na, "mode", item_list)

   // println("==================data body fill==================")
    //data_body_fill.take(50).foreach(i => println(i.mkString("||")))

    val after_data = FpmParse(data_body_fill, item_name, item_list)

    println("==================data body parsed================")
    after_data.take(50).foreach(i => println(i.mkString(" ")))

  //FP-Growth configuration
    val fpg = new FPGrowth()
    val model = fpg
      .setMinSupport(0.07)
      .setNumPartitions(4)
      .run(after_data)

    val freqItemsets = model.freqItemsets.collect().map { case (items, count) =>
      (items.toSet, count)
    }

    println("==========================FP-Growth Result============================")
    println("Freqent item number : " + freqItemsets.length)
    val freq_show = freqItemsets.foreach(i => println(i._1.map(i => {
        val j = i.split("=")
        j(2) + "=" + j(3)
    }).mkString(" ") + " : " + i._2.toString))

  }

}