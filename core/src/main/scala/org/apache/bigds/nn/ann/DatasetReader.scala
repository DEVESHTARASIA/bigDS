package org.apache.spark.mllib.bigds.ann

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector => SV, Vectors}
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
 * Created by clin3 on 2015/3/23.
 */
object DatasetReader {
  def read(sc: SparkContext, input: String, drop: Int, take: Int): (RDD[(SV, SV)], Int) = {
    val source = Source.fromFile(input)
    val lines = source.getLines()
    val n: Int = 3000

    val data = lines.map(line => {
      val splited = line.split(",").map(_.toDouble)
      var data = splited.take(10)
      val random = (for(i <- 0 until n) yield (scala.util.Random.nextInt(13) + 1).toDouble).toArray
      data = data ++: random
      /*
      for(i <- data) {
        print(i + " ")
      }
      print("\r\n")
      */
      val label = splited.last
      // println("label" + label)
      val x = new BDV(data)
      val y = BDV.zeros[Double](10)
      y(label.toInt) = 1D
      (Vectors.fromBreeze(x), Vectors.fromBreeze(y))
    }).toBuffer

    for(j <- 0 until 1000000) {
      val extra = (for(k <- 0 until n + 10) yield (scala.util.Random.nextInt(13) + 1).toDouble).toArray
      val x = new BDV(extra)
      val y = BDV.zeros[Double](10)
      y(0) = 1D
      val tuple = (Vectors.fromBreeze(x), Vectors.fromBreeze(y))
      data += tuple
    }
    (sc.parallelize(data.drop(drop).take(take).toSeq, 264), 3010)
  }

  def readTrain(sc: SparkContext, input: String, numSlices: Int): (RDD[(SV, SV)], Int) = {
    val rdd = sc.textFile(input, numSlices)
    (rdd.map(line => {
      line.split('|').map(x => {
        if(x.isEmpty) {
          0D
        } else {
          x.toDouble
        }
      })
    }).filter(x => x(1).toInt != 2).map(x => {
      val data = Vectors.dense(x.drop(4))
      val label = BDV.zeros[Double](2)
      label(x(1).toInt) = 1D
      (data, Vectors.fromBreeze(label))
    }), 1788)
  }

  def read1(sc: SparkContext, input: String, drop: Int, take: Int): (RDD[(SV, SV)], Int) = {
    val source = Source.fromFile(input)
    val lines = source.getLines()

    var trainData = lines.map(line => {
      val splited = line.split(",").map(_.toDouble)
      var i = 0
      var data: Array[Double] = new Array(0)
      splited.take(10).foreach(ele => {
        /*
        if(i == 0) {
          val arr: Array[Double] = new Array(4)
          arr(ele - 1) = 1D
          i = 1
          data = data ++: arr
        } else {
          val arr: Array[Double] = new Array(13)
          arr(ele - 1) = 1D
          i = 0
          data = data ++: arr
        }
        */
        data = data :+ ele
      })


      val random = (for(i <- 0 until 3000) yield (scala.util.Random.nextInt(13) + 1).toDouble).toArray
      data = data ++: random
      /*
      for(i <- data) {
        print(i + " ")
      }
      print("\r\n")
      */

      val label = splited.last
      // println("label" + label)
      val x = new BDV(data)
      assert(x.length == 3010)
      val y = BDV.zeros[Double](10)
      y(label.toInt) = 1D
      (Vectors.fromBreeze(x), Vectors.fromBreeze(y))
    }).toArray


    for(j <- 0 until 1000000) {
      val extra = (for(k <- 0 until 3000 + 10) yield (scala.util.Random.nextInt(13) + 1).toDouble).toArray
      val x = new BDV(extra)
      val y = BDV.zeros[Double](10)
      y(0) = 1D
      val tuple = (Vectors.fromBreeze(x), Vectors.fromBreeze(y))
      trainData = trainData :+ tuple
    }

    (sc.parallelize(trainData.drop(drop).take(take).toSeq, 528), 3010)
  }
}
