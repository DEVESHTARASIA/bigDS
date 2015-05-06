package com.intel.bigds.HealthCare.example

import com.intel.bigds.HealthCare.preprocessing.DataContainer
import com.intel.bigds.HealthCare.stat._
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.Set
import com.intel.bigds.HealthCare.stat


object KSTest {
  def Test(args: Array[String]): Unit = {
    println(args.mkString(","))
    println("KSTest tests check")
    if (args.length != 4) {
      System.err.println("4 parameters required: <spark master address> <numerical file address> <number of partitions> <BlankItems>")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster(args(0)).setAppName("KS test check")
    val sc = new SparkContext(conf)
    val num_address = args(1)
    val nparts = args(2).toInt

    val na = args(3).split(',').map(_.trim).toSet

    val num_data = sc.textFile(num_address, nparts).map(i => i.split(",")) //RDD[Array[String]]
    val data_filled = new DataContainer(num_data, na).allCleaning("Numerical", "mean").data
                                                 .map(i => i.zipWithIndex).flatMap(i => i).groupBy(i => i._2).map(i => (i._2.head._2, i._2.map(j => j._1)))
    val br_data = sc.broadcast(data_filled.collect())
    val result = data_filled.flatMap{ case (col,group) => {
        val paired_data = br_data.value.view.filter(i => i._1 > col)
        for (i <- paired_data) yield {
          (col, i._1, KSTwoSampleTest.ks_2samp_scipy(group.toArray.map(_.toDouble), i._2.toArray.map(_.toDouble)).pValue)
        }
      }
    }
    result.sortBy(_._3).take(10).foreach(i => println("Feature 1 " + i._1 + " and Feature 2 " + i._2 + " has pValue " + i._3 + "."))
  }
}