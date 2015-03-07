package kdTree

import org.apache.spark.Logging

/**
 * Created by datawlb on 2015/2/4.
 */
// TODO: KDNode structure may change according to future changes
class KDNode (
  val label: Int,
  //val pointData: List[Double],//Vector,//Product2[HyperPoint, A],
  val pointData: Array[Double],
  val splitAxis: Int,
  val range: Int,
  var isLeaf: Boolean,
  var leftNode: Option[KDNode],
  var rightNode: Option[KDNode],
  var parentNode: Option[KDNode]) extends Serializable with Logging {

  require(splitAxis < pointData.length)
  (leftNode, rightNode) match {
    case (None, None) =>
      this.isLeaf = true
      this.leftNode = Option.empty
      this.rightNode = Option.empty
    case (Some(_), None) =>
      //require()
      this.isLeaf = false
      this.rightNode = Option.empty
    case (Some(_), Some(_)) =>
      //require()
      this.isLeaf = false
    case _ =>
      throw new AssertionError("A KD Tree has never a node with just a right child!")
  }
  override def toString = "id = "
  def distance(point: Array[Double]): Double = {
    var dis = 0.0
    for (i <- 0 until(this.pointData.length)){
      val difference = (this.pointData(i) - point(i))
      dis = dis + difference * difference
    }
    math.sqrt(dis)
  }
}
object KDNode{
  /**
   * Return a node.
   */
  def apply(
    label: Int,
    //pointData: List[Double], //Product2[HyperPoint, A],
    pointData: Array[Double],
    splitAxis: Int,
    range: Int,
    isLeaf: Boolean,
    leftNode: Option[KDNode],
    rightNode: Option[KDNode],
    parentNode: Option[KDNode]): KDNode = {
    new KDNode(label, pointData, splitAxis, range, isLeaf, leftNode, rightNode, parentNode)
  }
}
