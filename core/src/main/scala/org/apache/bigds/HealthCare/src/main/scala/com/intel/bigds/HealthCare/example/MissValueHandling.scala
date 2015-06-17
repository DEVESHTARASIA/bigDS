package com.intel.bigds.HealthCare.example

import com.intel.bigds.HealthCare.preprocessing.DataContainer
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix}
import org.apache.spark.mllib.stat.test.PatchedTestResult
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import com.intel.bigds.HealthCare.stat._

import scala.collection.mutable


object MissValueHandling {

  def run(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Miss value handling")
      .registerKryoClasses(Array(classOf[Array[Double]], classOf[(Array[Double], Int)], classOf[(Int, Array[Double])]))

    val sc = new SparkContext(conf)

    val data = sc.textFile("./ref/testdata")
                 .map(i => i.split(","))
    val data_container = new DataContainer(data,Set("?"))
    val data_filled = data_container.allCleaning("Categorical","proportional")
    val former = data_container.data.map(i => i.mkString(",")).collect.mkString("\n")
    val after = data_filled.data.map(i => i.mkString(",")).collect.mkString("\n")
    println(former)
    println("==============================")
    println(after)
    println(data_container.data.count)
  }
  def main(args: Array[String]): Unit ={
    run(args)
  }
}