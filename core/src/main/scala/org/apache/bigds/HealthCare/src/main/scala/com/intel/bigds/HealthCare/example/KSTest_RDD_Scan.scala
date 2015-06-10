package com.intel.bigds.HealthCare.example

import com.intel.bigds.HealthCare.preprocessing.DataContainer
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix}
import org.apache.spark.mllib.stat.test.PatchedTestResult
import com.intel.bigds.HealthCare.stat._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.PriorityQueue
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._

import scala.collection.mutable


object KSTest_RDD_Scan {

  implicit object KSIndexOrdering extends Ordering[(Int, Int, KSTwoSampleTestResult)] with Serializable {
    def compare(a: (Int, Int, KSTwoSampleTestResult), b: (Int, Int, KSTwoSampleTestResult)) = if (a._3.pValue < b._3.pValue) -1 else if (a._3.pValue > b._3.pValue) 1 else 0
  }
/*
  implicit object KSOrdering extends Ordering[KSTwoSampleTestResult] with Serializable {
    def compare(a: KSTwoSampleTestResult, b: KSTwoSampleTestResult) = if (a.pValue < b.pValue) -1 else if (a.pValue > b.pValue) 1 else 0
  }
  */

  def main(args: Array[String]): Unit = {
    println(args.mkString(","))
    println("KSTest tests check")
    if (args.length != 5) {
      System.err.println("4 parameters required: <spark master address> <numerical file address> <number of partitions> <BlankItems> <topk>")
      System.exit(1)
    }
    val conf = new SparkConf()
                  .setMaster(args(0))
                  .setAppName("KS test check")
                  .registerKryoClasses(Array(classOf[Array[Double]], classOf[(Array[Double], Int)], classOf[(Int, Array[Double])]))

    val sc = new SparkContext(conf)
    val num_address = args(1)
    val nparts = args(2).toInt

    val na = args(3).split(',').map(_.trim).toSet
    val k = args(4).toInt

    val num_data = sc.textFile(num_address, nparts).map(i => i.split(",").tail) //RDD[Array[String]]

    val start = System.currentTimeMillis / 1000
    val data_filled = new DataContainer(num_data, na).allCleaning("Numerical", "mean").data.cache

    data_filled.count
    val middle = System.currentTimeMillis / 1000

    val data_col = data_filled.map(_.map(_.toDouble).zipWithIndex)

    val col_len = data_col.first.length
    val buf = mutable.ArrayBuilder.make[RDD[(Double, Int, Long)]]
    for (i <- 0 until col_len) {
     buf += data_col.map(j => j(i)).sortBy(_._1).zipWithIndex().map{case((value, col), index) => (value, col, index)}
    }
    val data_col_sort = buf.result()
    val res = new PriorityQueue[(Int, Int, KSTwoSampleTestResult)]
    for(i <- 0 until col_len) {
      for (j <- i+1 until col_len) {
        val rdd1 = data_col_sort(i)
        val rdd2 = data_col_sort(j)
        val testvalue = KSTwoSampleTest.ks_2samp_sc(rdd1, rdd2)
        if(res.size < k) {
          res.enqueue((i, j, testvalue))
        }
        else if (res.size >= k) {
          if (testvalue.pValue<res.head._3.pValue) {
            res.dequeue()
            res.enqueue((i,j,testvalue))
          }
        }
      }
    }
   val end =  System.currentTimeMillis / 1000

    println(res.toArray.mkString("\n"))
    println("*********************************************************************************")
    println("*********************************************************************************")
    println("Pre-processing costs " + (middle - start) + " s." )
    println("Statistical Tests cost " + (end - middle) + " s.")
    println("*********************************************************************************")
    println("*********************************************************************************")



    /* val br_data = sc.broadcast(data_filled.collect())
    val result = data_filled.flatMap{ case (col,group) => {
        val paired_data = br_data.value.view.filter(i => i._1 > col)
        for (i <- paired_data) yield {
          (col, i._1, KSTwoSampleTest.ks_2samp_scipy(group.toArray.map(_.toDouble), i._2.toArray.map(_.toDouble)).pValue)
        }
      }
    }
    result.sortBy(_._3).take(10).foreach(i => println("Feature 1 " + i._1 + " and Feature 2 " + i._2 + " has pValue " + i._3 + "."))
  }*/
  }
}
