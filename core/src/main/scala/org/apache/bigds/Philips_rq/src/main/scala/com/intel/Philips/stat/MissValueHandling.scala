package com.intel.Philips.stat

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.Set


object MissValueHandling {
  def main(args: Array[String]): Unit = {
    println("Missing data handling")
    if (args.length != 2){
      System.err.println("parameter required: <spark master address> <file address> ")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setMaster(args(0))
      //.setMaster("local[2]")
      .setAppName("Missing data handling")

    val sc = new SparkContext(conf)

    val file_address = args(1)
    val data = sc.textFile(file_address).map(i => i.split(",")).zipWithIndex().filter(_._2 > 0).map(_._1) //drop the first line of an RDD
    val data_container = new DataContainer(data)
    val na = Set("?")

    //Conduct "proportional" filling method test on column 41
    val Feature_num = 41
    val after_data = data_container.Cleaning(na, "proportional", Array(Feature_num))
    val show_data = data.zip(after_data)

    println("=============show the lost entries and filled result============")
    show_data.filter(i => i._1(Feature_num)=="?").map(i => i._1(Feature_num) + "//" + i._2(Feature_num)).collect().take(50).foreach(println)
    println("================================================================")

    println("=============compare the data before/after filling=============")
    println(show_data.count)
    show_data.map(i => i._1(Feature_num) + "//" + i._2(Feature_num)).collect().take(50).foreach(println)
    println("===============================================================")


    //Conduct "mean" or "median" filling method test on column 0. Because it's the only numerical column in given data
    //First we should manually create "lost entries".
    val lost_entry = sc.broadcast(Set(2,5,9,23,15))

    val data_abandon = data.map(i => {
      if(lost_entry.value.contains(i(0).toInt)){
        i(0) = "?"
      }
      i
    })
    val data_container_2 = new DataContainer(data_abandon)

    //val after_data_2 = data_container_2.Cleaning(na, "median", Array(0, 1))
    val after_data_2 = data_container_2.Cleaning(na, "mean", Array(0, 1))
    val show_data_2 = data_abandon.zip(after_data_2)
    println("=============show filling results=============")
    show_data_2.filter(i => i._1(0)=="?").map(i => i._1(0) + "//" + i._2(0)).collect().take(50).foreach(println)
    println("==============================================")

    sc.stop()
  }


}