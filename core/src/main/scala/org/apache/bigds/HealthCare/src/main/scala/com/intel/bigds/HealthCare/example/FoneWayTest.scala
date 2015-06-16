package com.intel.bigds.HealthCare.example

import com.intel.bigds.HealthCare.preprocessing.DataContainer
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.Set
import com.intel.bigds.HealthCare.stat._

object FoneWayTest {
  def run(args: Array[String]): FoneWayTestResult = {
    println(args.mkString(","))
    println("F one way test")
    if (args.length != 4){
      System.err.println("4 parameters required: <spark master address> <numerical file address> <number of partitions> <BlankItems>")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster(args(0)).setAppName("F one-way test check")
    val sc = new SparkContext(conf)

    val num_address = args(1)
    val nparts = args(2).toInt

    val na = args(3).split(',').map(_.trim).toSet

    val num_data = sc.textFile(num_address, nparts).map(i => i.split(",").tail) //RDD[Array[String]]

    val start = System.currentTimeMillis / 1000

    val data_container = new DataContainer(num_data, na).LengthCalculation
    val data_filled = data_container.allCleaning("Numerical", "mean")

    val FoneWayResult = FoneWay.FoneWayTest(data_filled.data, data_container.FeatureNum, data_container.ColLength)

    val end = System.currentTimeMillis / 1000
    println(FoneWayResult.toString)
    println("*********************************************************************************")
    println("*********************************************************************************")
    println("Test costs " + (end - start) + " s." )
    println("*********************************************************************************")
    println("*********************************************************************************")


    FoneWayResult
  }
  def main(args: Array[String]): Unit ={
    run(args)
  }
}