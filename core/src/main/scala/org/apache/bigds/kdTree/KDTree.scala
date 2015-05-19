package kdTree

import java.io.PrintWriter
//import java.util

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Vector}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._
import scala.io.Source
/**
 * Created by datawlb on 2015/2/5.
 */

class KDTree extends Serializable with Logging {
  var dim = 0
  var machines = 3 // exectors number
  var knn = 5 // knn default is 5

  def localCreateKDTree(subInput: Array[(Int, Array[Double])], depth: Int = 0, dim: Int,nodeID: Int, parentSplit: Double, parentsplitAxis: Int): Option[KDNode] = {
    subInput.length match {
      case 0 => Option.empty
      case 1 => Some(KDNode(nodeID, subInput.head._2, 0, subInput.head._1, true, None, None, parentSplit, parentsplitAxis))

      case subInputLength =>
        val splitAxis:Int = KDTree.getSplitAxis(depth, dim)
        val (left, rightWithMedian) = subInput.sortBy(_._2(splitAxis)).splitAt(subInputLength/2)
        val newDepth = depth+1
        val current = rightWithMedian.head
        val leftNode = localCreateKDTree(left, newDepth, this.dim, nodeID*2, current._2(splitAxis), splitAxis)
        val rightNode = localCreateKDTree(rightWithMedian.tail, newDepth, this.dim, nodeID*2+1, current._2(splitAxis), splitAxis)
        Some(KDNode(nodeID, current._2, splitAxis, current._1, false, leftNode, rightNode, parentSplit, parentsplitAxis))

    }
  }
  // use little tree get every node's tree, return RDD(little tree nodeID, rootNode belong to little tree nodeID)   RDD[(Long,Option[KDNode])]
  def run(input: RDD[(Int, Array[Double])], firstTreeModel: KDTreeModel): Unit = {
    val searchMap = scala.collection.mutable.Map[Int, Array[Double]]()
    firstTreeModel.topKDNode.get.preOrder(firstTreeModel.topKDNode, searchMap)
    val treeFindNeighbours = new KDTree()
    //var tt = new mutable.Seq[(Int, (Int, Array[Double]))] {}
    val tempArrayBF = new ArrayBuffer[(Int, (Boolean, (Int, Array[Double])))]()
    val nodesData = input.flatMap{ point =>
      tempArrayBF.clear()
      var disArray = new ArrayBuffer[(Int, Double)]()
      // TODO: use bbf search
      searchMap.foreach{sm =>
        var dis = 0.0
        for(j <- 0 until point._2.length){
          val difference = sm._2(j) - point._2(j)
          dis = dis + difference * difference
        }
        dis = math.sqrt(dis)
        disArray.append((sm._1, dis))
      }
      disArray = disArray.sortBy(_._2)
      tempArrayBF.append((disArray(0)._1, (true, point)))
      for(i <- 1 until(this.knn/2)){
        tempArrayBF.append((disArray(i)._1, (false, point)))
      }
      tempArrayBF

    }.groupByKey()  // input parall. distribute
    val subInputData = new ArrayBuffer[(Int, Array[Double])]()
    val subBuildTreeData = new ArrayBuffer[(Int, Array[Double])]()
    val results = nodesData.flatMap{ node =>
      subInputData.clear()
      node._2.foreach{ keyd =>
        if (keyd._1)
          subInputData.append(keyd._2)
        else
          subBuildTreeData.append(keyd._2)
      }
      //inputData = inputData++buildTreeData
      val localTree = localCreateKDTree((subInputData++subBuildTreeData).toArray, 0, this.dim, 1, 0, 0)
      subBuildTreeData.clear()
      val tempResult = subInputData.map{ data =>
        val kNeighbours = treeFindNeighbours.findNeighbours1(localTree, data._2, this.knn)
        //val kNeighbours = treeFindNeighbours.findNeighboursN2((inputData++buildTreeData).toArray, data._2, this.knn)
        (data._1, kNeighbours)
      }
      tempResult
      }.sortByKey().collect()
    val out = new PrintWriter("C:\\Users\\Administrator\\Desktop\\horizonDataResultParall.txt")///root/Desktop/horizonDataResult.txt
    results.foreach{res =>
      out.print(res._1-1)
      res._2.foreach{x =>
        out.print("\t")
        out.print(x.pointID-1)}
      out.println()}
    out.close()
    results
  }
  // use sample,generate little tree  Array[(Long, Option[KDNode])]
  def run(input: RDD[(Int, Array[Double])], p1: Int = 10): Unit ={
    val nodesSample = input.takeSample(false,p1,scala.util.Random.nextLong())
    //val firstTreeModel1 = run(input.sparkContext.parallelize(nodesSample))
    //val rdd1 = run(input, firstTreeModel)
    val firstTreeModel = localCreateKDTree(nodesSample.array, 0, this.dim, 1, 0, 0)

    run(input, new KDTreeModel(firstTreeModel,""))

    //rdd1.collect()
  }
  // general(N2) search
  def findNeighboursN2(data: Array[(Int, Array[Double])], searchPoint: Array[Double], k: Int = 1): ArrayBuffer[searchedPoint] = {
    val result = ArrayBuffer[searchedPoint]()
    for(i <- 0 until k){
      var dis = 0.0
      for (j <- 0 until searchPoint.length){
        val difference = data(i)._2(j) - searchPoint(j)
        dis = dis + difference * difference
      }
      dis = math.sqrt(dis)
      result.append(new searchedPoint(1, data(i)._1, data(i)._2, dis))
    }
    buildHeap(result)
    for(i <- k until data.length){
      var dis = 0.0
      for (j <- 0 until searchPoint.length){
        val difference = data(i)._2(j) - searchPoint(j)
        dis = dis + difference * difference
      }
      dis = math.sqrt(dis)
      if(dis < result(0).distance){
        result(0) = new searchedPoint(1, data(i)._1, data(i)._2, dis)
        maxHeap(result, 0, result.length)
      }
    }
    result

  }
  // BBF search
  def findNeighbours1(root: Option[KDNode], searchPoint: Array[Double], k: Int = 1): ArrayBuffer[searchedPoint] = {
    var isHeapBuild = false
    def searchToLeaf(currentNode: KDNode, searchPath: ArrayBuffer[KDNode], result: ArrayBuffer[searchedPoint]): Unit = {
      if(result.length < k){
        result.append(new searchedPoint(currentNode.id, currentNode.range, currentNode.pointData, currentNode.distance(searchPoint)))
      }
      else{
        if(!isHeapBuild){
          buildHeap(result)
          isHeapBuild = true
        }
        val dist = currentNode.distance(searchPoint)
        if(dist < result.head.distance){
          result(0) = new searchedPoint(currentNode.id, currentNode.range, currentNode.pointData, dist)
          maxHeap(result, 0, k)
        }
      }
      if(currentNode.pointData(currentNode.splitAxis) >= searchPoint(currentNode.splitAxis)){
        if(currentNode.rightNode.nonEmpty)
          searchPath.append(currentNode.rightNode.get)
        if(currentNode.leftNode.nonEmpty)
          searchToLeaf(currentNode.leftNode.get, searchPath, result)
      }else{
        if(currentNode.leftNode.nonEmpty)
          searchPath.append(currentNode.leftNode.get)
        if(currentNode.rightNode.nonEmpty)
          searchToLeaf(currentNode.rightNode.get, searchPath, result)
      }
    }

    val searchPath = ArrayBuffer[KDNode]()
    val result = ArrayBuffer[searchedPoint]()
    searchPath.append(root.get)
    while (searchPath.length > 0){
      val curNode = searchPath.remove(0)
      var isToLeaf = true
      if (result.length == k){
        //result.append(new searchedPoint(curNode.id, curNode.range, curNode.pointData, curNode.distance(searchPoint)))
        if(math.abs(curNode.parentNode-searchPoint(curNode.parentSplitAxis)) > result.head.distance)
          isToLeaf = false
      }
      isToLeaf = true
      if (isToLeaf)
        searchToLeaf(curNode, searchPath, result)
    }
    result
  }
  // general search
  def findNeighbours(root: Option[KDNode], searchPoint: Array[Double], k: Int = 1): ArrayBuffer[searchedPoint] = {
    require(searchPoint != null, "Argument 'searchPoint' must not be null")
    //require(searchPoint.dim == dim, "Dimension of 'searchPoint' (%d) does not match dimension of this tree (%d).".format(searchPoint.dim, dim))
    //require(k <= size, "k=%d must be <= size=%d".format(k, size))
    // var resultList = mutable.LinkedList[searchedPoint]()(k)
    val result = ArrayBuffer[searchedPoint]()
    val searchPath = ArrayBuffer[KDNode]()
    var tempNode = root
    // get first search path
    searchPath.append(tempNode.get)
    val scanNode = scala.collection.mutable.Map[Int, Boolean]()
    scanNode.put(tempNode.get.id, true)
      while (tempNode.nonEmpty && !tempNode.get.isLeaf){
        if (tempNode.get.pointData(tempNode.get.splitAxis) >= searchPoint(tempNode.get.splitAxis)){
          tempNode = tempNode.get.leftNode
        }else{
          tempNode = tempNode.get.rightNode
        }
        if (tempNode.nonEmpty){
          searchPath.append(tempNode.get)
          scanNode.put(tempNode.get.id, true)
        }
      }
    if (searchPath.isEmpty)
      return null
    // get first full result and then use heap sort
    while (result.length < k){
      if (searchPath.isEmpty)
        return null
      val tempNode = searchPath.remove(searchPath.length - 1)
      if (tempNode.leftNode.nonEmpty && !scanNode.getOrElse(tempNode.leftNode.get.id, false)) {
        searchPath.append(tempNode.leftNode.get)
        scanNode.put(tempNode.leftNode.get.id, true)
      }
      if (tempNode.rightNode.nonEmpty && !scanNode.getOrElse(tempNode.rightNode.get.id, false)) {
        searchPath.append(tempNode.rightNode.get)
        scanNode.put(tempNode.rightNode.get.id, true)
      }
      result.append(new searchedPoint(tempNode.id, tempNode.range, tempNode.pointData, tempNode.distance(searchPoint)))
    }
    if (searchPath.isEmpty || result.length < k){
      return null
    }
    buildHeap(result)
    // search from first of the searchPath until empty
    def search(searchPath: ArrayBuffer[KDNode]): Unit ={

      val currentNode = searchPath.remove(searchPath.length - 1)
      val currentNodeDistance = currentNode.distance(searchPoint)
      if (currentNodeDistance < result.head.distance){
        result(0) = new searchedPoint(currentNode.id, currentNode.range, currentNode.pointData, currentNodeDistance)
        maxHeap(result, 0, result.length)
        if (currentNode.leftNode.nonEmpty && !scanNode.getOrElse(currentNode.leftNode.get.id, false)){
          searchPath.append(currentNode.leftNode.get)
          scanNode.put(currentNode.leftNode.get.id, true)
        }
        if (currentNode.rightNode.nonEmpty && !scanNode.getOrElse(currentNode.rightNode.get.id, false)){
          searchPath.append(currentNode.rightNode.get)
          scanNode.put(currentNode.rightNode.get.id, true)
        }
      }
      if (searchPath.nonEmpty)
        search(searchPath)
    }
    search(searchPath)
    result
  }

  // heapsort
  def buildHeap(arr:ArrayBuffer[searchedPoint]) {
    //((arr.length/2.0D).floor.toInt-1 until -1 by -1).foreach( i => maxHeap(arr, i, arr.length) )
    (arr.length/2 until -1 by -1).foreach( i =>
      maxHeap(arr, i, arr.length) )
  }
  def maxHeap(arr:ArrayBuffer[searchedPoint], idx:Int, max:Int) {
    val l = left(idx)
    val r = right(idx)
    var largest = if (l < max && arr(l).distance > arr(idx).distance) l else idx
    largest = if (r < max && arr(r).distance > arr(largest).distance) r else largest
    if (largest != idx) {
      swap(arr, idx, largest)
      maxHeap(arr, largest, max)
    }
  }
  private def parent(idx:Int):Int = idx/2
  private def left(idx:Int):Int = 2*idx+1
  private def right(idx:Int):Int = (2*idx)+2
  def swap(s: ArrayBuffer[searchedPoint], i: Int, j: Int):Unit = {
    val v = s(i)
    s(i) = s(j)
    s(j) = v
  }

}

object KDTree extends Serializable with Logging {

  def apply(dataInput: RDD[(Int, Array[Double])]): Unit ={   // Array[(Long, Option[KDNode])]
    val kdTree = new KDTree()
    kdTree.dim = dataInput.first()._2.length
    kdTree.run(dataInput, 10)
  }

  def getSplitAxis(depth: Int, dim: Int): Int = {
    if (depth==2)
      depth & 1
    else
      depth % dim
  }

  def main(args: Array[String]) {

    val points = new ArrayBuffer[(Int, Array[Double])]()
    var pointID: Int = 0
    for (line <- Source.fromFile("C:\\Users\\Administrator\\Desktop\\horizonData01.dat").getLines){    // ("/root/Desktop/horizonData.dat").getLines){    // /home/wanglb/horizonData.dat
    val tempArray = line.split("\t").map(x => x.toDouble).splitAt(2)._2
      pointID = pointID + 1
      points.append((pointID, tempArray))
    }

    val sparkConf = new SparkConf().setAppName("kdTree")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(points, 40)
    val z = KDTree(rdd)
    sc.stop()
    val treeFindNeighbours = new KDTree()

    println("success")
  }
}
// TODO: 1.the result(knn point) have itself,should remove,2. Need test in general data, 3.can make a compare with ANN
//*/