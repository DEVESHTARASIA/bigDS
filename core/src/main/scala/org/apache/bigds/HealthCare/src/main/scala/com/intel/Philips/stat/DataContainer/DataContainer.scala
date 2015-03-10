package com.intel.bigds.stat

import breeze.linalg.min
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Set
import org.apache.spark.SparkContext._
import scala.util.Random
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object DataContainer {

  def To_LabeledData(dataarray: RDD[Array[String]], feature_pos: Array[Int], label_pos: Int): RDD[LabeledPoint] = {

    val feature_double = (for (k <- feature_pos) yield {
      dataarray.map(i => i(k)).distinct().collect.zipWithIndex.map(i => (i._1, i._2.toDouble)).toMap
    }).toArray

    val label_double = dataarray.map(i => i(label_pos)).distinct().collect.zipWithIndex.map(i => (i._1, i._2.toDouble)).toMap

    val res = dataarray.map(i => {
      var j = 0
      val featureArray = (for (k <- feature_pos) yield {
        val reflect = feature_double(j)(i(k))
        j += 1
        reflect
      }).toArray
      new LabeledPoint(label_double(i(label_pos)), Vectors.dense(featureArray))
    })
    res
  }
}

class DataContainer(val data: RDD[Array[String]]) extends Serializable {

  val func = (aiter : Iterator[Array[String]], biter: Iterator[Int]) => {
    val seed = biter.next()
    aiter.map(i => (i, seed))
  }
  //CleanMethod: (replace by) mean, median, mode, zero ; abandon
  def Cleaning(na: Set[String], CleaningMethod: String, Feature_num_Array: Array[Int]): RDD[Array[String]] = {
    var after_data:RDD[Array[String]] = data
    for (m <- Feature_num_Array) {
      if (CleaningMethod == "mean") {
        val data_filter = after_data.filter(i => !na.contains(i(m)))
        val pre_data = data_filter.map(i => (i(m).toDouble,1)).reduce((a,b) => ((a._1 + b._1), (a._2 + b._2)))
        val avg = pre_data._1 / pre_data._2
        val br_avg = data.sparkContext.broadcast(avg)
        after_data = after_data.map(i => {
          if (na.contains(i(m))){
            i(m) = br_avg.value.toString
          }
          i
        }
        )
      }
      else if (CleaningMethod == "median") {
        val data_filter = after_data.filter(i => !na.contains(i(m)))
        val sorted_data = data_filter.map(i => i(m).toDouble).sortBy(a => a).zipWithIndex().map {
          case (v, idx) => (idx, v)
        }
        val count = sorted_data.count()
        //more functional programming style
        val median: Double = if (count % 2 == 0) {
          val l = count / 2 -1
          val r = l + 1
          (sorted_data.lookup(l).head + sorted_data.lookup(r).head) / 2
        } else sorted_data.lookup(count / 2).head

        val br_median = data.sparkContext.broadcast(median)
        after_data = after_data.map(i => {
          if (na.contains(i(m))) {
            i(m) = br_median.value.toString
          }
          i
        })
      }
      //fill missing values according to the probability of known entries of the existing data
      else if (CleaningMethod == "proportional") {
        val data_filter = after_data.filter(i => !na.contains(i(m))).cache()
        val data_size = data_filter.count()
        val br_size = data.sparkContext.broadcast(data_size)
        var j = 0
        val ratio = data_filter.map(i => (i(m), 1)).reduceByKey(_ + _).map(i => (i._1, i._2.toDouble / br_size.value)).collect.map{ case (a, b) => {
          val result = (a, (j, min(10000,(j + (10000 * b).ceil.toInt))))
          j = (j + 10000 * b).ceil.toInt
          result
        }}
        //println("=======================================")
        // ratio.foreach(println)
        // println("=======================================")
        val br_ratio = data.sparkContext.broadcast(ratio)
        val npart = data.partitions.length // Are there better ways to get the number of parititions of an RDD ?
        val rnd = new Random(23)
        val seed = data.sparkContext.parallelize(Range(0, npart).map(i => rnd.nextInt), npart)
        val data_seed = after_data.zipPartitions(seed)(func)
        after_data = data_seed.mapPartitions(iter => {
          iter.map(i => {
            val rnd_sub = new Random(i._2)
            val j = i._1
            if (na.contains(j(m))) {
              val random_gen = rnd_sub.nextInt(10000)
              val replace_item = br_ratio.value.filter((x: (String, (Int, Int))) => random_gen >= x._2._1 && random_gen < x._2._2).map(p => p._1)
              //val replace_item = watch.toString
              j(m) = replace_item(0)
            }
            j
          })
        })
      }

      else { //abandon corresponding row
        after_data = after_data.flatMap(i => {
          if (na.contains(i(m))) {
            Nil
          }
          else {
            List(i)
          }
        })
      }
    }
  after_data
  }
}




