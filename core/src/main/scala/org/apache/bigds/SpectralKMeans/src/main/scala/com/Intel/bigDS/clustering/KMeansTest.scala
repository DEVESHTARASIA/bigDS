package com.Intel.bigDS.clustering


import org.apache.spark.mllib.clustering.{KMeans, SpectralKMeans}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.clustering.KMeans.{K_MEANS_PARALLEL, RANDOM}


object KMeansTest extends Serializable {
  def run(args: Array[String]): (Array[(Int, Vector)], Array[(Int,Int)]) = {
    println("KMeans normal referece")
    if (args.length != 6) {
      System.err.println("ERROR:Spectral Clustering: <spark master> <path to data> <nParts> <sparsity> <sigma> <number of clusters>")
    }
    println("===========================" + args.mkString(",") + "===============================")
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("KMeans Clustering as reference")
    @transient val sc = new SparkContext(conf)

    val data_address = args(1)
    val nParts = args(2).toInt
    val sparsity = args(3).toDouble
    val sigma = args(4).toDouble
    val numcluster = args(5).toInt
    val br_nametolabel = sc.broadcast(NametoLabel)
    val parsed = sc.textFile(data_address, nParts).map(_.split(",").map(_.toDouble)).map(Vectors.dense(_)).distinct.cache()

    val numDim = parsed.count
    val numfeatures = parsed.first.size
    //val model = SpectralKMeans.train(parsed, numcluster, numDim.toInt, sparsity, 100, 1, K_MEANS_PARALLEL, 29)
    val start = System.currentTimeMillis / 1000
    val model = KMeans.train(parsed, numcluster, 100)
    val end = System.currentTimeMillis / 1000
    val N = parsed.count
    val ASE_res = model.computeCost(parsed)

    val centers = model.clusterCenters.zipWithIndex.map(i => (i._2,i._1))
    val center_num = model.predict(parsed).map(i => (i,1)).groupByKey.map(i => (i._1, i._2.size)).collect()


    println("*********************************************************************************")
    println("*********************************************************************************")
    println("Training costs " + (start - end) + " seconds")
    println("*********************************************************************************")
    println("*********************************************************************************")
    println(ASE_res)
    println("WSSSE value")

    (centers,center_num)





  }
  def main(args:Array[String]): Unit = {
    run(args)
  }
}