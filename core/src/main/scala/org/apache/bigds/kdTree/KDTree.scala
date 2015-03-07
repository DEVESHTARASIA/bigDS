package kdTree

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by datawlb on 2015/2/5.
 */
///**
class KDTree extends Serializable with Logging {
  var dim = 0
  def run(input: RDD[LabeledPoint]): KDTreeModel = {
    this.dim = input.first().features.size
    val kdNode: Option[KDNode] = createKDTree(input.collect(), 0, this.dim)
    new KDTreeModel(kdNode,"")
  }
  def createKDTree(subInput: Array[LabeledPoint], depth: Int = 0, dim: Int): Option[KDNode] = {
    subInput.length match {
      case 0 => Option.empty
      case 1 => Some(KDNode(subInput.head.label.toInt, subInput.head.features.toArray, 0, 1, true, None, None, None))

      case subInputLength =>
        val splitAxis:Int = KDTree.getSplitAxis(depth, dim)
        val (left, rightWithMedian) = subInput.sortBy(_.features(splitAxis)).splitAt(subInputLength/2)
        val newDepth = depth+1
        val current = rightWithMedian.head
        val leftNode = createKDTree(left, newDepth, this.dim)
        val rightNode = createKDTree(rightWithMedian.tail, newDepth, this.dim)
        Option(KDNode(current.label.toInt, current.features.toArray, splitAxis, 1, false, leftNode, rightNode, None))

    }
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
      result.append(new searchedPoint(tempNode.label, tempNode.pointData, tempNode.distance(searchPoint)))
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
        result(0) = new searchedPoint(currentNode.label, currentNode.pointData, currentNodeDistance)
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

  def apply(dataInput: RDD[LabeledPoint]): KDTreeModel ={
    val kdTree = new KDTree()
    kdTree.run(dataInput)
  }

  def getSplitAxis(depth: Int, dim: Int): Int = {
    if (depth==2)
      depth & 1
    else
      depth % dim
  }

  def main(args: Array[String]) {
    val points = new Array[LabeledPoint](6)
    points(0) = new LabeledPoint(0.0, Vectors.dense(2, 3))
    points(1) = new LabeledPoint(0.0, Vectors.dense(5, 4))
    points(2) = new LabeledPoint(0.0, Vectors.dense(9, 6))
    points(3) = new LabeledPoint(0.0, Vectors.dense(4, 7))
    points(4) = new LabeledPoint(0.0, Vectors.dense(8, 1))
    points(5) = new LabeledPoint(0.0, Vectors.dense(7, 2))
    val sparkConf = new SparkConf().setAppName("DecisioinTree")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(points)
    val tree = KDTree(rdd)
    val treeFindNeighbours = new KDTree()
    val testFindResult = treeFindNeighbours.findNeighbours(tree.topKDNode, Array(2,4.5), 3)
    testFindResult.foreach(x => println(x.distance))
    //val nodeList1 = tree.findNeighbours(HyperPoint(2, 4.5), k = 2) map (e => e.point)
    //val expected1 = List[HyperPoint](points(2))
  }
}

  //*/