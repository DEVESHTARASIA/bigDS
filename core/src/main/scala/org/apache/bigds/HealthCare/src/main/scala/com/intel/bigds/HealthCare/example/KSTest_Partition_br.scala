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


object KSTest_Partition_br {
  /*implicit object KSOrdering extends Ordering[KSTwoSampleTestResult] with Serializable {
    def compare(a: KSTwoSampleTestResult, b: KSTwoSampleTestResult) = if (a.pValue < b.pValue) -1 else if (a.pValue > b.pValue) 1 else 0
  }
*/
  implicit object KSIndexOrdering extends Ordering[(Int, Int, KSTwoSampleTestResult)] with Serializable {
    def compare(a: (Int, Int, KSTwoSampleTestResult), b: (Int, Int, KSTwoSampleTestResult)) = if (a._3.pValue < b._3.pValue) -1 else if (a._3.pValue > b._3.pValue) 1 else 0
  }

  def blockify(features: RDD[(Int, Array[Double])], nPart: Int): RDD[(Int, Array[(Int, Array[Double])])] = {
    val featurePartitioner = new HashPartitioner(nPart)
    val blockedFeatures = features.map { row =>
      (featurePartitioner.getPartition(row._1), row)
    }.groupByKey(nPart).map {
      case (col, rows) => (col, rows.toArray)
    }
    blockedFeatures.count()
    blockedFeatures
  }

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
    //sc.addJar("lib/commons-math3-3.3.jar")
    val num_address = args(1)
    val nparts = args(2).toInt

    val na = args(3).split(',').map(_.trim).toSet
    val k = args(4).toInt

    val num_data = sc.textFile(num_address, nparts).map(i => i.split(",").tail) //RDD[Array[String]]

    val start = System.currentTimeMillis / 1000

    val data_filled = new DataContainer(num_data, na).allCleaning("Numerical", "mean").data.cache
    //materialize data_filled to judge filling time
    data_filled.count()

    val middle = System.currentTimeMillis / 1000

    val data_col = data_filled.map(_.map(_.toDouble))
        .flatMap(i => i.zipWithIndex).groupBy(i => i._2).map(i => (i._2.head._2, i._2.map(j => j._1)))
        .map(i => (i._1, i._2.toArray))

    val blocked_data = blockify(data_col, nparts)
    var res_data = blocked_data.map{ case (blocknum, elearray) => (blocknum, elearray, new mutable.PriorityQueue[(Int, Int, KSTwoSampleTestResult)])}
    for (p <- 0 until nparts) {
      val partRdd_judge = blocked_data.filter(i => i._1==p).collect
      if(!partRdd_judge.isEmpty) {
        val partRdd = partRdd_judge.head
        val br_pd = sc.broadcast(partRdd)
        val br_p = sc.broadcast(p)
        res_data = res_data.map { i => {
          if (i._1 >= br_p.value) {
            val partialdata = br_pd.value
            for (m <- i._2; n <- partialdata._2 if m._1 != n._1) {
              val testvalue = KSTwoSampleTest.ks_2samp_math(m._2, n._2)
              if (i._3.size < k) {
                i._3.enqueue((m._1, n._1, testvalue))
              }
              else if (i._3.size >= k) {
                if (testvalue.pValue < i._3.head._3.pValue) {
                  i._3.dequeue()
                  i._3.enqueue((m._1, n._1, testvalue))
                }
              }
            }
            i
          }
          else i
        }
        }
      }

    }

    val res_show = res_data.flatMap(i => i._3.toSeq).sortBy(_._3.pValue).takeSample(true, k, 29).map(i => "Random samples are:" + i._1 + "," + i._2 + "\n" + i._3)
    //why toArray here will cause a implicit ambiguous error?
    //println(res_data.flatMap(i => i._3.toSeq).sortBy(_._3.pValue).take(k).map(i => i._1 + "," + i._2 + "\n" + i._3).mkString("\n================\n"))
    val end = System.currentTimeMillis / 1000

    println(res_show.mkString("\n================\n"))

    println("*********************************************************************************")
    println("*********************************************************************************")
    println("Pre-processing costs " + (middle - start) + " s." )
    println("Statistical Tests cost " + (end - middle) + " s.")
    println("*********************************************************************************")
    println("*********************************************************************************")




    //data_col
/*
    val parts = data_col.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = data_col.mapPartitionsWithIndex((index, element) => if (index == idx) element else Iterator(), true)
      val PartialData = partRdd.collect

      val br_pd = sc.broadcast(PartialData)
      data_col = data_col.mapPartitions { iter => {
      */



    }


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
