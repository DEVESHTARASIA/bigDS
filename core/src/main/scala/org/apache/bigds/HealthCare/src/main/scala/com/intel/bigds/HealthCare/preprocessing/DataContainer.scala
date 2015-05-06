package com.intel.bigds.HealthCare.preprocessing

import breeze.linalg.min
import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Set
import org.apache.spark.SparkContext._
import scala.collection.mutable
import scala.util.Random
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.collection.mutable.{Map, HashMap}
import org.apache.spark.mllib.stat.{PatchedStatistics, MultivariateStatisticalSummary}
import scala.collection.mutable.Map




object DataContainer {


  def ConvertToLabeledData(dataarray: RDD[Array[String]], feature_pos: Array[Int], label_pos: Int): RDD[LabeledPoint] = {

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
//DataAggregate: aggregate each column to get the number of each categories in each feature, in order to do the two-sample correlation test followed
  def DataAggregate(data:RDD[Array[String]]):(mutable.HashMap[Int, String], Array[Array[Double]]) = {
    val attribute_num = data.first.length
    //val category = mutable.Map[String, Int]()
    val zero = new Array[mutable.Map[String, Int]](attribute_num).map(i => mutable.Map[String, Int]())
    val aggregate_result = data.treeAggregate(zero)(
      seqOp = (U, r) => {
        //r:Array(cat1, cat2, cat3, ....)    U: Array(col1, col2, col3, ...)
        val U2 = r.zip(U).map { case (item, aggregation) => {
          aggregation(item) = aggregation.getOrElse(item, 0) + 1
          aggregation
        }
        }
        U2
      },
      combOp = (U1, U2) => {
        val U = U1.zip(U2).map{case (u1, u2) => {
          val list = u1.toList ++ u2.toList
          val merged = list.groupBy(_._1).map{case (k,v) => (k,v.map(_._2).sum)}
          val result = mutable.Map[String, Int]()
          merged.map{case (a,b) => result(a) = b}
          result
        }
        }
        U
      }
    )
    //aggregate_result: Array[Map[String, Int]]
    //return Array[Array[Int]]
    val categories = mutable.Set[String]()
    aggregate_result.map{i => {
      i.keySet.map(i => categories.add(i))
    }
    }
    val categories_num = categories.size
    val catIdHash = new mutable.HashMap[Int, String]
    categories.toArray.zipWithIndex.map{ case(cat_name, index) => catIdHash(index) = cat_name}
    val result = new Array[Array[Double]](attribute_num)
    for (k <- 0 until attribute_num) {
      result(k) = new Array[Double](categories_num)
      for (m <- 0 until categories_num) {
        result(k)(m) = aggregate_result(k).getOrElse(catIdHash(m),0).toDouble
      }
    }
    (catIdHash, result)
  }

  //def PairScan(val aggregated_data:Array[Array[Int]]) {

 // }

}

class DataContainer(var data: RDD[Array[String]], na:Set[String]) extends Serializable {
  data.cache()
  val FeatureNum = data.first.length
  var ColFullLength:Long = _
  var ColLength:Array[Long] = _
  //val ColLength:Array[Int] = new Array[Int](FeatureNum).map(i => ColFullLength)
  val func = (aiter : Iterator[Array[String]], biter: Iterator[Int]) => {
    val seed = biter.next()
    aiter.map(i => (i, seed))
  }
  def LengthCalculation {
    ColFullLength=data.count
    ColLength = new Array[Int](FeatureNum).map(i => ColFullLength)
  }
  //CleanMethod: (replace by) mean, median, proportional, zero ; abandon
  //to implement: column-wise info one time + map filling
  def allCleaning(InputType:String, method:String) = {
    val seed = new Random(29)
    if (InputType == "Numerical") {
      val parsed_data = data.map(_.map { i => {
        if (na.contains(i)) {
          Double.MaxValue
        }
        else {
          try {
            i.toDouble
          }
          catch {
            case e: Exception => println("ERROR: Incorrect parameter types. Should be Double")
          }
        }
        0.0
      }
      }).map(Vectors.dense(_))
      val summary: MultivariateStatisticalSummary = PatchedStatistics.colStats(parsed_data)
      data = data.map(i => {
        i.zipWithIndex.map { case (j, inx) => {
          if (na.contains(j)) {
            method match {
              case "mean" => summary.mean(inx).toString
              case "max" => summary.max(inx).toString
              case "min" => summary.min(inx).toString
              case "random" => {
                (seed.nextGaussian() * summary.variance(inx) + summary.mean(inx)).toString
              }
              case _ => throw new IllegalArgumentException("Indicated method does not exist.")
            }
          }
          else j
        }
        }
      })
    }
    else if (InputType == "Categorical") {
      if (method == "proportional") {
        val data_replace_nan = data.map(i => i.map(j => if (na.contains(j)) "NAN" else j))
        var j = 0
        val Proportion_list = data_replace_nan.aggregate(new Array[HashMap[String, Int]](FeatureNum))(
          seqOp = (U, V) => {
            //V: Array[String]
            val V_inx = V.zipWithIndex
            for (m <- V_inx) {
              U(m._2).update(m._1, U(m._2).getOrElse(m._1, 0) + 1)
            }
            U
          },
          combOp = (V1, V2) => {
            V1.zip(V2).map { case (map1, map2) => {
              val list = map1.toList ++ map2.toList
              val merged = list.groupBy(_._1).map { case (k, v) => k -> v.map(_._2).sum}
              merged
            }
            }
            V1
          }
        ).map(i => {
          val i_filter = i.filter(_._1 != "NAN")
          val denominor = i_filter.map(_._2).sum
          i_filter.map { case (a, b) =>
            val result = (a, (j, min(10000, (j + (10000 * b.toDouble / denominor).ceil.toInt))))
            j = (j + 10000 * b.toDouble / denominor).ceil.toInt
            result
          }
        })

        data = data.map(i => {
          i.zipWithIndex.map { case (item, inx) => {
            var res: String = null
            if (na.contains(item)) {
              val rand_gen = seed.nextInt(10000)
              res = Proportion_list(inx).filter((x: (String, (Int, Int))) => rand_gen >= x._2._1 && rand_gen < x._2._2).map(p => p._1).head
            }
            else res = item
            res
          }
          }
        })
      }
      else if(method=="abandon"){
        data = data.map(i => {
          i.filter(!na.contains(_))
        })
      }
      else throw new Exception("INdicated method does not exist.")
    }
    else throw new Exception("Indicated data type does not exist.")
    this
  }
  def ColCleaning(CleaningMethod: String, Feature_num_Array: Array[Int])  = {
    //var after_data:RDD[Array[String]] = data
    for (m <- Feature_num_Array) {
      require(m>=0 && m<FeatureNum, s"feature index exceeds limit" )
      if (CleaningMethod == "mean") {
        val data_filter = data.filter(i => !na.contains(i(m)))
        val pre_data = data_filter.map(i => (i(m).toDouble,1)).reduce((a,b) => ((a._1 + b._1), (a._2 + b._2)))
        val avg = pre_data._1 / pre_data._2
        val br_avg = data.sparkContext.broadcast(avg)
        data = data.map(i => {
          if (na.contains(i(m))){
            i(m) = br_avg.value.toString
            ColLength(m) -= 1
          }
          i
        }
        )
      }
      else if (CleaningMethod == "median") {
        val data_filter = data.filter(i => !na.contains(i(m)))
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
        data = data.map(i => {
          if (na.contains(i(m))) {
            i(m) = br_median.value.toString
            ColLength(m) -= 1
          }
          i
        })
      }
      //fill missing values according to the probability of known entries of the existing data
      else if (CleaningMethod == "proportional") {
        val data_filter = data.filter(i => !na.contains(i(m))).cache()
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
        val npart = data.partitions.length
        val rnd = new Random(23)
        val seed = data.sparkContext.parallelize(Range(0, npart).map(i => rnd.nextInt), npart)
        val data_seed = data.zipPartitions(seed)(func)
        data = data_seed.mapPartitions(iter => {
          iter.map(i => {
            val rnd_sub = new Random(i._2)
            val j = i._1
            if (na.contains(j(m))) {
              val random_gen = rnd_sub.nextInt(10000)
              val replace_item = br_ratio.value.filter((x: (String, (Int, Int))) => random_gen >= x._2._1 && random_gen < x._2._2).map(p => p._1)
              //val replace_item = watch.toString
              j(m) = replace_item(0)
              ColLength(m) -= 1
            }
            j
          })
        })
      }
      else if(CleaningMethod=="abandon"){ //abandon corresponding row
        data = data.flatMap(i => {
          if (na.contains(i(m))) {
            ColLength.map(i => i - 1)
            Nil
          }
          else {
            List(i)
          }
        })
      }
      else throw new Exception("Indicated method does not exist.")
    }
    this
  }

/*
For unbinned data, Dataplot automatically generates binned data using the same rule as for histograms.
That is, the class width is 0.3*s where s is the sample standard deviation. The upper and lower limits
are the mean plus or minus 6 times the sample standard deviation (any zero frequency bins in the tails
are omitted).
Imitating description at http://www.itl.nist.gov/div898/software/dataplot/refman1/auxillar/chi2samp.htm

So to my understanding, there will be (6+6)s / 0.3s = 40 binned categories
 */
  def Binning() {
    val DoubleData = data.map(i => i.map(_.toDouble))
    val data_vector = DoubleData.map(i => Vectors.dense(i))
    val summary: MultivariateStatisticalSummary = PatchedStatistics.colStats(data_vector)
    val br_summary = data.sparkContext.broadcast(summary)
    data = DoubleData.map(i => i.zipWithIndex.map { case (data, index) => RangeDefinition(data, index).toString})
      //1~40
    def RangeDefinition(data: Double, index: Int): Int = {
      val mean = br_summary.value.mean.toArray(index)
      val deviation = sqrt(br_summary.value.variance.toArray(index))
      //var binned = 0
      val begin = mean - 6 * deviation
      val step = deviation * 0.3
      for (i <- 0 until 40) {
        if ((data >= begin + i * step) && (data < begin + (i + 1) * step)) {
          return i
        }
        else if (data < begin) {
          return -1
        }
        else if (data >= mean + 6 * deviation) {
          return 100
        }
      }
        -2
      }
    }

}




