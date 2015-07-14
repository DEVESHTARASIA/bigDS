import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform._
import scala.reflect.ClassTag
import scala.util.control.Breaks
import org.apache.spark.Partitioner

/**
 * Created by clin3 on 2015/6/4.
 */
class PPCTree[Item: ClassTag](
    val numFreqItems: Int,
    val minCount: Long,
    val rankToItem: Map[Int, Item]) extends Serializable {
  import PPCTree._
  val root: PPCTreeNode = new PPCTreeNode
  var itemsetCount = new Array[Int]((this.numFreqItems - 1) * this.numFreqItems / 2)
  val nodeListBegin = new Array[Int]((this.numFreqItems - 1) * this.numFreqItems / 2)
  val nodeListLen = new Array[Int]((this.numFreqItems - 1) * this.numFreqItems / 2)
  val headTable = new Array[PPCTreeNode](this.numFreqItems)
  val headTableLen = new Array[Int](this.numFreqItems)
  var supportDict: Array[Int] = _
  var PPCTreeNodeCount = 0
  var nodeListRowIndex = 0
  var firstNodeListBegin = 0
  var bfCursor = 0
  var bfRowIndex = 0
  val bfSize = 1000000
  var bfCurrentSize = this.bfSize * 10
  val bf = new Array[Array[Int]](100000)
  bf(0) = new Array[Int](bfCurrentSize)
  var resultLen = 0
  val result = Array[Int](numFreqItems)
  var level1: Double = 0
  var initial: Double = 0
  var nodelistCount = 0
  var bfCount = 0

  def add(t: Array[Int]): this.type = {
    var curPos = 0
    var curRoot = root
    var rightSibling: PPCTreeNode = null
    val outerLoop = new Breaks()
    outerLoop.breakable(
      while(curPos != t.size) {
        var child = curRoot.firstChild
        val innerLoop = new Breaks()
        innerLoop.breakable(
          while(child != null) {
            if(child.label == t(curPos)) {
              curPos += 1
              child.count += 1
              curRoot = child
              innerLoop.break()
            }
            if(child.rightSibling == null) {
              rightSibling = child
              child = null
              innerLoop.break()
            }
            child = child.rightSibling
          }
        )
        if(child == null) {
          outerLoop.break()
        }
      }
    )
    for(i <- curPos until t.size) {
      val node = new PPCTreeNode()
      node.label = t(i)
      if(rightSibling != null) {
        rightSibling.rightSibling = node
        rightSibling = null
      } else {
        curRoot.firstChild = node
      }
      node.rightSibling = null
      node.labelSibling = null
      node.firstChild = null
      node.parent = curRoot
      node.count = 1
      curRoot = node
      PPCTreeNodeCount += 1
    }
    this
  }

  def genNodeList(id: Int, partitioner: Partitioner): this.type = {
    val tempHead = new Array[PPCTreeNode](this.numFreqItems)
    var curRoot = root.firstChild
    var pre = 0
    var last = 0
    while(curRoot != null) {
      curRoot.foreIndex = pre
      pre += 1
      if(headTable(curRoot.label) == null) {
        headTable(curRoot.label) = curRoot
        tempHead(curRoot.label) = curRoot
      } else {
        tempHead(curRoot.label).labelSibling = curRoot
        tempHead(curRoot.label) = curRoot
      }
      headTableLen(curRoot.label) += 1
      var temp = curRoot.parent
      while(temp.label != -1) {
        itemsetCount(curRoot.label * (curRoot.label - 1) / 2 + temp.label) += curRoot.count
        temp = temp.parent
      }
      if(curRoot.firstChild != null) {
        curRoot = curRoot.firstChild
      } else {
        // back visit
        curRoot.backIndex = last
        last += 1
        if(curRoot.rightSibling != null) {
          curRoot = curRoot.rightSibling
        } else {
          curRoot.backIndex = last
          last += 1
          if(curRoot.rightSibling != null) {
            curRoot = curRoot.rightSibling
          } else {
            curRoot = curRoot.parent
            val loop = new Breaks()
            loop.breakable(
              while(curRoot != null) {
                // back visit
                curRoot.backIndex = last
                last += 1
                if(curRoot.rightSibling != null) {
                  curRoot = curRoot.rightSibling
                  loop.break()
                }
                curRoot = curRoot.parent
              }
            )
          }
        }
      }
    }
    this
  }

  def mine(
      rankToCount: Array[(Int, Long)],
      id: Int,
      partitioner: Partitioner): ArrayBuffer[(Array[Item], Long)] = {
    val tree = new SearchTree(this)
    val startTime = currentTime
    tree.initialize(rankToCount)
    val endTime = currentTime
    val totalTime: Double = endTime - startTime
    initial = totalTime / 1000
    tree.mine(this.bfCursor, this.bfRowIndex, this.bfCurrentSize, id, partitioner)
  }
}

object PPCTree {
  class PPCTreeNode extends Serializable {
    var label: Int = -1
    var firstChild: PPCTreeNode = null
    var rightSibling: PPCTreeNode = null
    var labelSibling: PPCTreeNode = null
    var parent: PPCTreeNode = null
    var count: Int = -1
    var foreIndex: Int = -1
    var backIndex: Int = -1
  }
}
