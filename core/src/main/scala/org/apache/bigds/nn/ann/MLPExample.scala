package org.apache.spark.mllib.bigds.ann

import org.apache.spark.mllib.bigds.pmml.{MLPImporter, MLPExporter}
import org.apache.spark.mllib.bigds.sensitivity.{MLPAnalyser, SensitivityAnalyser}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Matrices

/**
 * Created by clin3 on 2015/3/14.
 */
object MLPExample {
  def main(args: Array[String]): Unit = {
    val batchSize = args(0).toInt
    val maxNumIterations = args(1).toInt
    val convergenceTol = args(2).toDouble
    val numSlices = args(3).toInt


    //Initialize SparkConf.
    val conf = new SparkConf()
    conf
      .setAppName("MLPExample")
      .set("spark.cores.max", "264")
      .set("spark.executor.memory", "160g")
      .set("spark.akka.frameSize", "1000")
      .set("spark.driver.maxResultSize", "2g")
      .setMaster("spark://sr471:7180")
    //Initialize SparkContext.
    val sc = new SparkContext(conf)


    // Load training set from disk.
    // val (data, numVisible) = MnistDatasetSuite.mnistTrainDataset(sc, input, 5000)
    // val (data, numVisible) = DatasetReader.read1(sc, "poker-hand/poker-hand-training-true.data", 0, 1000000)
    val (data, numVisible) = DatasetReader.readTrain(sc, "hdfs://sr471:54311/user/clin/paypal/ato_train.csv", numSlices)
    val (valid, _) = DatasetReader.readTrain(sc, "hdfs://sr471:54311/user/clin/paypal/ato_valid.csv", numSlices)
    // println("numVisible = " + numVisible)
    /*
    val arr = new Array[Double](numVisible)
    var i = 0
    val temp = data.collect()
    val y = temp(0)._1
    println("x.length = " + temp.size)
    println("y.length = " + y.size)

    for(j <- 0 until numVisible) {
      val x = y(j)
      arr(i) = x
      i = i + 1
      print(x + " ")
    }
    print("\r\n")

    println("label:")
    for(index <- 0 until 10) {
      print(temp(0)._2(index) + " ")
    }
    print("\r\n")
    */

    // Define the structure of neural network.
    val topology = Array(numVisible, 30, 2)
    // Train neural network.
    // val nn = MLP.train(data, 20, 1000, topology, 0.05, 0.1, 0.0)
    val nn = MLP.runLBFGS(data, valid, topology, batchSize, maxNumIterations, convergenceTol, 0)
    // MLP.runSGD(data, nn, 37, 6000, 0.1, 0.5, 0.0)


    // Load test set from disk.
    // val (dataTest, _) = MnistDatasetSuite.mnistTrainDataset(sc, input, 1000000, 10000)
    // val (dataTest, _) = DatasetReader.readPaypal(sc, "hdfs://sr471:54311/user/clin/paypal/ato_valid.csv", 1000000, 10000)
    val (dataTest, _) = DatasetReader.readTrain(sc, "hdfs://sr471:54311/user/clin/paypal/ato_test.csv", numSlices)

    // Output error.
    // val (dataTest, num) = DatasetReader.read1(sc, "poker-hand/poker-hand-training-true.data", 1000000, 5000)
    println("Error: " + MLP.error(dataTest, nn, 100))

    /*
    val mlpAnalyser = new MLPAnalyser(nn)

    val sensitivity = mlpAnalyser.analyse(data)

    println("sensitivity.numRows = " + sensitivity.rows + " cols = " + sensitivity.cols)
    for(i <- 0 until sensitivity.cols) {
      print(sensitivity(0, i) + " ")
    }
    print("\r\n")
    */

    /*
    nn.toPMML(s"/home/yilan/LinChen/mlp.xml")

    MLPImporter.importMLP(s"/home/yilan/LinChen/mlp.xml", arr)
    print("Expected = ")

    val result = nn.predict(Matrices.dense(numVisible, 1, y.toArray))
    for(i <- result.toArray) {
      print(i + " ")
    }


    print("\r\n")

    val w = nn.innerLayers(0).weight
    for(i <- 0 until w.numCols) {
      println(w(0, i))
    }
    */
  }
}