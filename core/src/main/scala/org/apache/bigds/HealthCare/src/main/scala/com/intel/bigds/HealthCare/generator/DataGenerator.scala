package com.intel.bigds.HealthCare.generator

import breeze.numerics.abs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random


/**
 * Created by yaochunnan on 3/19/15.
 */
object DataGenerator {

  def getRangeRDD(sc: SparkContext, rand: Random, size: Int, parts: Int): RDD[(Int, Int, Int)] = {

    val avg = size / parts
    val fullNum = size % parts
    val seedArray = ArrayBuffer[Int]()
    val rseeds = Range(0, parts).map { i => (i, rand.nextInt) }.toArray
    //val rseeds = Range(0, parts).map { i => (i, (for(i<-0 until nbatch) yield rand.nextInt)) }.toArray
    sc.parallelize(rseeds, parts).map {
      case (i, seed) =>
        if (i < fullNum) (i * (avg + 1), (i + 1) * (avg + 1), seed)
        else (i * avg + fullNum, (i + 1) * avg + fullNum, seed)
    }
  }

  def main(args: Array[String]): Unit = {
//blank items: "NAN, ,NA,nothing,."
    println(args.length)
    println(args.mkString(","))
    if (args.length != 9) {
      System.err.println("ERROR: expected 9 args\n<spark master> <path of data> <number of partitions> <number of records> <number of features> <numerical table name> <categorical table name> <lost ratio> <blank items>")
      System.exit(1)
    }
    var DATA_PATH = args(1)
    val nparts = args(2).toInt
    val nRecords = args(3).toInt
    val NumericalTable = args(5)
    val CategoricalTable = args(6)
    val LostRatio = args(7).toDouble
    val BlankItem = args(8).split(',').map(_.trim)


    val conf = new SparkConf().setMaster(args(0)).setAppName("HealthCare Data Generator")

    val sc = new SparkContext(conf)
    val nFeatures = sc.broadcast(args(4).toInt)
    val br_blanklength = sc.broadcast(BlankItem.length)

    if(! DATA_PATH.endsWith("/")){
      println("WARNING: path may cause errors. This should be a folder with '/'. Attempting to fix.")
      DATA_PATH = DATA_PATH + "/"
    }

    try {
      ("hadoop fs -rmr " + DATA_PATH + NumericalTable).!
      ("hadoop fs -rmr " + DATA_PATH + CategoricalTable).!
    }catch{
      case e: Exception => println("indicated path does not exist!")
    }
    val globalRandom = new Random(23)
    val DataRange = getRangeRDD(sc, globalRandom, nRecords, nparts)

    println(">>> Generating NumericalTable with labels (0/1) at top ......")
    DataRange.flatMap {
      case (startx, endx, rinit) =>
        val rand = new Random(rinit)
        for (i <- startx until endx) yield {
          val miss_entries = for (k <- 0 until (nFeatures.value * LostRatio).toInt) yield {
            rand.nextInt(nFeatures.value)
          }
          val rand_data = for (j <- 0 until nFeatures.value) yield {
            if (miss_entries.contains(j)){
              val blank_inx = rand.nextInt(br_blanklength.value)
              BlankItem(blank_inx)
            }
            else {
              j%3 match {
                case 0 => abs(rand.nextGaussian()).toString
                case 1 => abs(rand.nextDouble() * 0.5 + 3).toString
                case 2 => abs(rand.nextDouble()).toString
              }
              //abs(rand.nextGaussian()).toString()
            }
          }
          rand.nextInt(2).toString() + "," + rand_data.mkString(",")
        }
    }.saveAsTextFile(DATA_PATH + NumericalTable)

    println(">>> Generating Categorical Table .....")
    DataRange.flatMap {
      case (startx, endx, rinit) =>
        val rand = new Random(rinit)
        for (i <- startx until endx) yield {
          val miss_entries = for (k <- 0 until (nFeatures.value * LostRatio).toInt) yield {
            rand.nextInt(nFeatures.value)
          }
          val rand_data = for (j <- 0 until nFeatures.value) yield {
            if (miss_entries.contains(j)){
              val blank_inx = rand.nextInt(br_blanklength.value)
              BlankItem(blank_inx)
            }
            else rand.nextInt(20).toString()
          }
          rand_data.mkString(",")
        }
    }.saveAsTextFile(DATA_PATH + CategoricalTable)

  }


}
