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


object KSTest {

  def blockify(features: RDD[(Int, Array[Double])], nPart: Int): RDD[(Int, Array[(Int, Array[Double])])] = {
    val featurePartitioner = new HashPartitioner(nPart)
    val blockedFeatures = features.map { row =>
      (featurePartitioner.getPartition(row._1), row)
    }.groupByKey(nPart).map {
      case (col, rows) => (col, rows.toArray)
    }
    //blockedFeatures.count()
    blockedFeatures
  }

  def multiply(
                small: RDD[(Int, Array[Double])],
                big: RDD[(Int, Array[Double])],
                function: (Array[Double], Array[Double]) => PatchedTestResult,
                topk: Int): RDD[(Int, Int, Double)] = {

    //val ord = Ordering[(Float, Long)].on[(Long, Double)](x => (x._2.toFloat, x._1))
    val defaultParallelism = big.sparkContext.defaultParallelism

    //why divide by a half
    val smallBlocks = math.sqrt(math.max(small.sparkContext.defaultParallelism, small.partitions.size)).ceil.toInt
    val bigBlocks = math.sqrt(math.max(defaultParallelism, big.partitions.size)).ceil.toInt
   // val smallBlocks = 27
  //  val bigBlocks = 27

    val blockedSmall = blockify(small, smallBlocks)
    val blockedBig = blockify(big, bigBlocks)

    blockedSmall.setName("blockedSmallMatrix")
    blockedBig.setName("blockedBigMatrix")
    blockedBig.count()
    blockedBig.cache()

    val topkSims = blockedBig.cartesian(blockedSmall).flatMap {
      case ((bigBlockIndex, bigRows), (smallBlockIndex, smallRows)) =>
        val buf = mutable.ArrayBuilder.make[(Int, (Int, Double))]
        for (i <- 0 until bigRows.size; j <- 0 until smallRows.size if smallRows(j)._1 != bigRows(i)._1) {
          val bigIndex = bigRows(i)._1
          val bigRow = bigRows(i)._2
          val smallIndex = smallRows(j)._1
          val smallRow = smallRows(j)._2
          println("================================begin test==================================")
          val testval = function(bigRow, smallRow).pValue
          println("================================end test====================================")
          //val testval = 0.1
          val entry = (bigIndex, (smallIndex, testval))
          buf += entry
        }
        buf.result()
    }.sortBy(_._2._2).map { case (bigIndex, (smallIndex, value)) =>
      (bigIndex, smallIndex, value)
    }.repartition(defaultParallelism)

    blockedBig.unpersist()

    topkSims.count()

   topkSims
  }

  def run(args: Array[String]): RDD[(Int,Int,Double)] = {
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
   // sc.addJar("lib/commons-math3-3.3.jar")
    val num_address = args(1)
    val nparts = args(2).toInt

    val na = args(3).split(',').map(_.trim).toSet
    val k = args(4).toInt

    val num_data = sc.textFile(num_address, nparts).map(i => i.split(",").tail) //RDD[Array[String]]

    val start = System.currentTimeMillis()/1000

    val data_filled = new DataContainer(num_data, na).allCleaning("Numerical", "mean").data.cache()
    data_filled.count

    val middle = System.currentTimeMillis()/1000

    val data_col = data_filled.map(_.map(_.toDouble))
        .flatMap(i => i.zipWithIndex).groupBy(i => i._2).map(i => (i._2.head._2, i._2.map(j => j._1)))
        .map(i => (i._1, i._2.toArray))
    //data_col


    val statres = multiply(data_col, data_col, KSTwoSampleTest.ks_2samp_math, -1)
    val res_show = statres.takeSample(true, 50,29)
    val end = System.currentTimeMillis()/1000

    println(res_show.map(i => "Random samples are: " + i._1 + "," + i._2 + "," + i._3).mkString("\n"))
    println("*********************************************************************************")
    println("*********************************************************************************")
    println("Pre-processing costs " + (middle - start) + " s." )
    println("Statistical Tests cost " + (end - middle) + " s.")
    println("*********************************************************************************")
    println("*********************************************************************************")

    statres

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
  def main(args: Array[String]): Unit ={
    run(args)
  }
}