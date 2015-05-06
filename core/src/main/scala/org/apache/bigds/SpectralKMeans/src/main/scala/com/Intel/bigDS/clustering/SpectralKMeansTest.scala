package com.Intel.bigDS.clustering


import org.apache.spark.mllib.clustering.SpectralKMeans
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans.{K_MEANS_PARALLEL, RANDOM}

object SpectralKMeansTest extends Serializable {
  val NametoLabel = Map("C15" -> 0, "CCAT" -> 1, "E21" -> 2, "ECAT" -> 3, "GCAT" -> 4, "M11" -> 5)
  def main(args: Array[String]): Unit = {
    println("Spectral KMeans on RCV1&RCV2 data")
    if (args.length != 4) {
      System.err.println("ERROR:Spectral Clustering: <spark master> <path to data> <nParts> <sparsity>")
    }
    println("===========================" + args.mkString(",") + "===============================")
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("Spectral Clustering")
    @transient val sc = new SparkContext(conf)

    val data_address = args(1)
    val nParts = args(2).toInt
    val sparsity = args(3).toDouble
    val br_nametolabel = sc.broadcast(NametoLabel)

    //parse data
    val parsed = sc.textFile(data_address, nParts)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .mapPartitions { iter =>
      val ntl = br_nametolabel.value
      iter.map { line =>
        val items = line.split(' ')
        val label = ntl(items.head).toDouble
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
        val value = indexAndValue(1).toDouble
          (index, value)
        }.unzip
        (label, indices.toArray, values.toArray)
      }
    }
    val numFeatures = {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      parsed.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }
    val parseddata = parsed.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
    }

    val numDim = parseddata.count
    val model = SpectralKMeans.train(parseddata.map(_.features), 6, numDim.toInt, sparsity, 100, 1, K_MEANS_PARALLEL, 29)

    val valuesAndPreds = parseddata.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }.groupByKey.map(i => (i._1, i._2.map((_,1)).groupBy(_._1).map(j => (j._1, j._2.map(_._2).sum)).toMap)).collect
    //val ACC = valuesAndPreds.collect.sum.toDouble / numDim
    println("==============Clustering Quality Observation===============")
    println(valuesAndPreds.mkString("\n"))
    //println("Accuracy of clustering is " + ACC + ".")
    //model.predict(parseddata.map(_.features)).map(_.toDouble)

  }
}