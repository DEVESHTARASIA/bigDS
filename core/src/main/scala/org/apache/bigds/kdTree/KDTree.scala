package kdTree

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Vector}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._
import scala.io.Source

/**
 * Created by datawlb on 2015/2/5.
 */
///**
class KDTree extends Serializable with Logging {
  var dim = 0
  def run(input: RDD[LabeledPoint]): KDTreeModel = {
    this.dim = input.first().features.size
    var depth: Int = 0
    val dataLength = input.count().toInt
    val splitAxis: Int = KDTree.getSplitAxis(0, this.dim)
    val (leftPoints, rightWithMedianPoints) = input.collect().sortBy(_.features(splitAxis)).splitAt(dataLength / 2)
    val currentNode = rightWithMedianPoints.head
    var rootNode: Option[KDNode] = Some(KDNode(1, currentNode.label.toInt, currentNode.features.toArray, 0, 1, false, None, None, None))
    var currentParentNodes = ArrayBuffer(rootNode.get)
    //currentParentNodes.append(rootNode.get)

    val labeledInput = input.zipWithIndex().map(x => (x._2, x._1))
    val inputMap = labeledInput.map(point => (1.toLong, point))
    var keys = new Array[(Long, Int)](input.count().toInt)
    for (i <- 0 to keys.length-1){
      keys(i) = (1, splitAxis)
    }
    while (currentParentNodes.nonEmpty){
      keys = createKDTree(labeledInput, keys, depth , this.dim, dataLength, rootNode, currentParentNodes)
      depth = depth + 1
      updateCurrentParentNodes(currentParentNodes)
    }
    //val kdNode: Option[KDNode] = createKDTree1(input.collect(), 0, this.dim)             ,Option[KDNode],ArrayBuffer[KDNode]   Array[LabeledPoint]
    new KDTreeModel(rootNode,"")
  }

  //def createKDTree(subInput: Iterable[(Long, LabeledPoint)], nodeID: Long, depth: Int = 0, dim: Int, rootNode: Option[KDNode], currentParentNodes: ArrayBuffer[KDNode]): (ArrayBuffer[(Long, Long)]) = {
  def createKDTree(labeledInput: RDD[(Long, LabeledPoint)], keys: Array[(Long, Int)], depth: Int = 0, dim: Int, dataLength: Int, rootNode: Option[KDNode], currentParentNodes: ArrayBuffer[KDNode]): Array[(Long, Int)] = {

    def updateKeys(keyPoints: ((Long, Int), Iterable[(Long, LabeledPoint)]), currentParentNodes: ArrayBuffer[KDNode]): ArrayBuffer[(Long, (Long, Int))] = {
        val nodeID = keyPoints._1._1
      if (nodeID >=currentParentNodes.head.id){
        val subInputArray = keyPoints._2.toArray
        val (pointsNB, pointsData) = keyPoints._2.unzip
        val subInput1 = pointsData.toArray
        val subInputLength = subInputArray.length
        if (subInputLength == 1) {
          ArrayBuffer((subInputArray.head._1, keyPoints._1))
        }else{
            val splitAxis: Int = KDTree.getSplitAxis(depth, dim)
            val (leftPoints, rightWithMedianPoints) = subInputArray.sortBy(_._2.features(splitAxis)).splitAt(subInputLength / 2)
            val newDepth = depth + 1
            val currentPoint = rightWithMedianPoints.head
            val leftArrayBuffer = new ArrayBuffer[(Long, (Long, Int))]()
            val rightArrayBuffer = new ArrayBuffer[(Long, (Long, Int))]()
            leftPoints.map(labeledPoint => (labeledPoint._1, (nodeID * 2, splitAxis))).copyToBuffer(leftArrayBuffer)
            rightWithMedianPoints.tail.map(labeledPoint => (labeledPoint._1, (nodeID * 2 + 1, splitAxis))).copyToBuffer(rightArrayBuffer)
            //(ArrayBuffer((nodeID, currentPoint)) ++ leftArrayBuffer ++ rightArrayBuffer, rootNode, currentParentNodes)
            ArrayBuffer((currentPoint._1, keyPoints._1)) ++ leftArrayBuffer ++ rightArrayBuffer
          }
          //ArrayBuffer((nodePoints._2.head._1, nodePoints._1))
      }else{
        ArrayBuffer((keyPoints._2.head._1, keyPoints._1))
      }

    }
      val input = labeledInput.sparkContext.parallelize(keys).zip(labeledInput)
      val keyPoints  = input.groupByKey()
        .flatMap(nodePoints =>
        updateKeys(nodePoints, currentParentNodes)
        ).sortByKey()
      val newKeyPoints = keyPoints.values.collect()
    if (depth > 0){
      val minID = currentParentNodes.head.id * 2
      val maxID = currentParentNodes.last.id * 2 + 1
      val currentChildNodes =  keyPoints.values.zip(labeledInput).filter(point => point._1._1>=minID && point._1._1<=maxID).collectAsMap()
      var isLeaf: Boolean = false
      if (currentChildNodes.head._1._1 > dataLength/2){
        isLeaf = true
      }
      currentParentNodes.foreach{ node =>
        val id = node.id
        val leftTemp = currentChildNodes.find(point => point._1._1==id*2)
        if (leftTemp.nonEmpty){
          node.leftNode = Some(KDNode(leftTemp.get._1._1, leftTemp.get._2._2.label.toInt, leftTemp.get._2._2.features.toArray, leftTemp.get._1._2, 1, isLeaf,None, None,None))
        }
        val rightTemp = currentChildNodes.find(point => point._1._1==id*2+1)
        if (rightTemp.nonEmpty){
          node.rightNode = Some(KDNode(rightTemp.get._1._1, rightTemp.get._2._2.label.toInt, rightTemp.get._2._2.features.toArray, rightTemp.get._1._2, 1, isLeaf,None, None,None))
        }
      }
    }
      newKeyPoints
    }

  def updateCurrentParentNodes(currentParentNodes: ArrayBuffer[KDNode]): Unit ={
    val arrayLength = currentParentNodes.length
    if (currentParentNodes.head.leftNode.nonEmpty || currentParentNodes.head.rightNode.nonEmpty){
      for (i <- 0 to arrayLength-1){
        val removeNode = currentParentNodes.remove(0)
        if (!removeNode.isLeaf){
          if (removeNode.leftNode.nonEmpty)
            currentParentNodes.append(removeNode.leftNode.get)
          if (removeNode.rightNode.nonEmpty)
            currentParentNodes.append(removeNode.rightNode.get)
        }
      }
    }
  }
  def localCreateKDTree(subInput: Array[LabeledPoint], depth: Int = 0, dim: Int,nodeID: Long): Option[KDNode] = {
    subInput.length match {
      case 0 => Option.empty
      case 1 => Some(KDNode(nodeID, subInput.head.label.toInt, subInput.head.features.toArray, 0, 1, true, None, None, None))

      case subInputLength =>
        val splitAxis:Int = KDTree.getSplitAxis(depth, dim)
        val (left, rightWithMedian) = subInput.sortBy(_.features(splitAxis)).splitAt(subInputLength/2)
        val newDepth = depth+1
        val current = rightWithMedian.head
        val leftNode = localCreateKDTree(left, newDepth, this.dim, nodeID*2)
        val rightNode = localCreateKDTree(rightWithMedian.tail, newDepth, this.dim, nodeID*2+1)
        Option(KDNode(nodeID, current.label.toInt, current.features.toArray, splitAxis, 1, false, leftNode, rightNode, None))

    }
  }
  // use little tree get every node's tree, return RDD(little tree nodeID, rootNode belong to little tree nodeID)
  def run(input: RDD[LabeledPoint], firstTreeModel: KDTreeModel): RDD[(Long,Option[KDNode])] = {
    val treeFindNeighbours = new KDTree()
    val allNodesData = input.map{ point =>
      val kNeighbours = treeFindNeighbours.findNeighbours(firstTreeModel.topKDNode,point.features.toArray,1)
      (kNeighbours.head.id, point)
    }.groupByKey()
    .map{ nodesData =>
      val nodeTreeModel = localCreateKDTree(nodesData._2.toArray, 0, this.dim, 1)
      (nodesData._1, nodeTreeModel)
    }
    allNodesData
  }
  // use sample,generate little tree
  def run(input: RDD[LabeledPoint], p1: Int = 10): Array[(Long, Option[KDNode])] ={
    val nodesSample = input.takeSample(false,p1,scala.util.Random.nextLong())
    val firstTreeModel = run(input.sparkContext.parallelize(nodesSample))
    val rdd1 = run(input, firstTreeModel)
    rdd1.collect()
  }
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
    while (!tempNode.get.isLeaf){
      if (tempNode.get.pointData(tempNode.get.splitAxis) >= searchPoint(tempNode.get.splitAxis)){
        tempNode = tempNode.get.leftNode
      }else{
        tempNode = tempNode.get.rightNode
      }
      if (tempNode.nonEmpty)
        searchPath.append(tempNode.get)
    }
    if (searchPath.isEmpty)
      return null
    // get first full result and then use heap sort
    while (result.length < k){
      if (searchPath.isEmpty)
        return null
      val tempNode = searchPath.remove(searchPath.length - 1)
      if (tempNode.leftNode.nonEmpty)
        searchPath.append(tempNode.leftNode.get)
      if (tempNode.rightNode.nonEmpty)
        searchPath.append(tempNode.rightNode.get)
      result.append(new searchedPoint(tempNode.id, tempNode.label, tempNode.pointData, tempNode.distance(searchPoint)))
    }
    if (searchPath.isEmpty || result.length < k){
      return null
    }
    buildHeap(result)
    // search from first of the searchPath until empty
    def search(searchPath: ArrayBuffer[KDNode]): Unit ={
      if (searchPath.isEmpty)
        return
      val currentNode = searchPath.remove(searchPath.length - 1)
      val currentNodeDistance = currentNode.distance(searchPoint)
      if (currentNodeDistance < result.head.distance){
        result(0) = new searchedPoint(currentNode.id, currentNode.label, currentNode.pointData, currentNodeDistance)
        maxHeap(result, 0, result.length)
        if (currentNode.leftNode.nonEmpty)
          searchPath.append(currentNode.leftNode.get)
        if (currentNode.rightNode.nonEmpty)
          searchPath.append(currentNode.rightNode.get)
      }
      search(searchPath)
    }
    search(searchPath)
    result
  }

  // heapsort
  def buildHeap(arr:ArrayBuffer[searchedPoint]) {
    ((arr.length/2.0D).floor.toInt-1 until -1 by -1).foreach( i => maxHeap(arr, i, arr.length) )
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
  private def parent(idx:Int):Int = (idx/2.0D).floor.toInt
  private def left(idx:Int):Int = 2*idx+1
  private def right(idx:Int):Int = (2*idx)+2
  def swap(s: ArrayBuffer[searchedPoint], i: Int, j: Int):Unit = {
    val v = s(i)
    s(i) = s(j)
    s(j) = v
  }

}

object KDTree extends Serializable with Logging {

  def apply(dataInput: RDD[LabeledPoint]): Array[(Long, Option[KDNode])] ={
    val kdTree = new KDTree()
    kdTree.run(dataInput, 10)
  }

  def getSplitAxis(depth: Int, dim: Int): Int = {
    if (depth==2)
      depth & 1
    else
      depth % dim
  }

  def main(args: Array[String]) {
    /**
    val points = new Array[LabeledPoint](6)
    points(0) = new LabeledPoint(0.0, Vectors.dense(2, 3))
    points(1) = new LabeledPoint(0.0, Vectors.dense(5, 4))
    points(2) = new LabeledPoint(0.0, Vectors.dense(9, 6))
    points(3) = new LabeledPoint(0.0, Vectors.dense(4, 7))
    points(4) = new LabeledPoint(0.0, Vectors.dense(8, 1))
    points(5) = new LabeledPoint(0.0, Vectors.dense(7, 2))
      */
    val points = new ArrayBuffer[LabeledPoint]()
    for (line <- Source.fromFile("C:\\Users\\Administrator\\Desktop\\horizonData.dat").getLines){
      val tempArray = line.split("\t").map(x => x.toDouble).splitAt(2)._2
      points.append(new LabeledPoint(0, new DenseVector(tempArray)))
    }

    val sparkConf = new SparkConf().setAppName("kdTree")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(points).cache()
    val z = KDTree(rdd)
    sc.stop()
    val treeFindNeighbours = new KDTree()
//    val testFindResult = treeFindNeighbours.findNeighbours(tree.topKDNode, Array(2,4.5), 1)

//    testFindResult.foreach(x => println(x.distance))
    //val nodeList1 = tree.findNeighbours(HyperPoint(2, 4.5), k = 2) map (e => e.point)
    //val expected1 = List[HyperPoint](points(2))
    println("")
  }
}

//*/