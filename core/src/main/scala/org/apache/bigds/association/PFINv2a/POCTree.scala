package org.apache.spark.armlib.fpm

import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks

private[fpm] class POCTree[Item: ClassTag](val numFreqItems: Int, val minCount: Long, val rankToItem: Map[Int, Item]) extends Serializable {

  import org.apache.spark.armlib.fpm.POCTree._

  val root: POCTreeNode = new POCTreeNode()
  var itemsetCount = new Array[Int]((this.numFreqItems - 1) * this.numFreqItems / 2)
  val nodesetBegin = new Array[Int]((this.numFreqItems - 1) * this.numFreqItems / 2)
  val nodesetLen = new Array[Int]((this.numFreqItems - 1) * this.numFreqItems / 2)
  var supportDict: Array[Int] = _
  var POCTreeNodeCount = 0
  var nodesetRowIndex = 0
  var firstNodesetBegin = 0
  var bfCursor = 0
  var bfRowIndex = 0
  val bfSize = 1000000
  var bfCurrentSize = this.bfSize * 10
  val bf = new Array[Array[Int]](100000)
  var resultLen = 0
  val result = Array[Int](numFreqItems)

  def add(t: Array[Int]): this.type = {
    var curPos = 0
    var curRoot = root
    var rightSibling: POCTreeNode = null
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
      val node = new POCTreeNode()
      node.label = t(i)
      if(rightSibling != null) {
        rightSibling.rightSibling = node
        rightSibling = null
      } else {
        curRoot.firstChild = node
      }
      node.rightSibling = null
      node.firstChild = null
      node.parent = curRoot
      node.count = 1
      curRoot = node
      POCTreeNodeCount += 1
    }
    this
  }

  def gen2Nodesets(id: Int, partitioner: Partitioner): this.type = {
    // Count 2-itemset by traversing the POCTree
    this.supportDict = new Array[Int](this.POCTreeNodeCount + 1)
    var curRoot = root.firstChild
    var pre = 0
    while(curRoot != null) {
      curRoot.foreIndex = pre
      this.supportDict(pre) = curRoot.count
      pre += 1
      if(partitioner.getPartition(curRoot.label) == id) {
        var temp: POCTreeNode = curRoot.parent
        while(temp.label != -1) {
          this.itemsetCount(curRoot.label * (curRoot.label - 1) /  2 + temp.label) += curRoot.count
          nodesetLen(curRoot.label * (curRoot.label - 1) / 2 + temp.label) += 1
          temp = temp.parent
        }
      }

      if(curRoot.firstChild != null) {
        curRoot = curRoot.firstChild
      } else {
        if(curRoot.rightSibling != null) {
          curRoot = curRoot.rightSibling
        } else {
          curRoot = curRoot.parent
          val loop = new Breaks()
          loop.breakable(
            while(curRoot != null) {
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

    // Generate the nodesets of frequent 2-itemsets
    var sum = 0
    for(i <- 0 until (this.numFreqItems - 1) * this.numFreqItems / 2) {
      if(this.itemsetCount(i) >= this.minCount) {
        this.nodesetBegin(i) = sum
        sum += this.nodesetLen(i)
      }
    }

    if(this.bfCursor + sum > this.bfCurrentSize * 0.85) {
      this.bfCursor = 0
      this.bfCurrentSize = sum + 1000
    }
    this.bf(this.bfRowIndex) = new Array[Int](this.bfCurrentSize)
    this.nodesetRowIndex = this.bfRowIndex
    this.firstNodesetBegin = this.bfCursor
    curRoot = this.root.firstChild
    this.bfCursor += sum
    while(curRoot != null) {
      var temp = curRoot.parent
      while(temp.label != -1) {
        if(this.itemsetCount(curRoot.label * (curRoot.label - 1) / 2
          + temp.label) >= this.minCount) {
          val cursor = this.nodesetBegin(curRoot.label * (curRoot.label - 1) / 2 + temp.label)
            + this.firstNodesetBegin
          this.bf(this.nodesetRowIndex)(cursor) = curRoot.foreIndex
          this.nodesetBegin(curRoot.label * (curRoot.label - 1) / 2 + temp.label) += 1
        }
        temp = temp.parent
      }
      if(curRoot.firstChild != null) {
        curRoot = curRoot.firstChild
      } else {
        if(curRoot.rightSibling != null) {
          curRoot = curRoot.rightSibling
        } else {
          curRoot = curRoot.parent
          val loop = new Breaks()
          loop.breakable(
            while(curRoot != null) {
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
    for(i <- 0 until this.numFreqItems * (this.numFreqItems - 1) / 2) {
      if(this.itemsetCount(i) >= this.minCount) {
        this.nodesetBegin(i) = this.nodesetBegin(i) - this.nodesetLen(i)
      }
    }
    this
  }

  def mine(rankToCount: Array[(Int, Long)], id: Int, partitioner: Partitioner): ArrayBuffer[(Array[Item], Long)] = {
    val patternTree = new PatternTree(this)
    patternTree.initialize(rankToCount)
    patternTree.mine(this.bfCursor, this.bfRowIndex, this.bfCurrentSize, id, partitioner)
  }

/* def merge(other: POCTree[T]): this.type = {
    other
  }

  def transactions: Iterator[(List[T], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(curRoot: POCTreeNode[T]): Iterator[(List[T], Long)] = {
    var count = curRoot.count
    var node = curRoot.firstChild
    val iterator = Iterator.single((Nil, count))
    while(node != null) {
      val item = node.label
      val child = node.firstChild
      getTransactions(child).map{ case (t, c) =>
        count -= c
        (item :: t, c)
      }
      node = node.rightSibling
    }
    iterator ++ {
      if(count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }*/

}

private[fpm] object POCTree {
  class POCTreeNode extends Serializable {
    var label: Int = -1
    var firstChild: POCTreeNode = null
    var rightSibling: POCTreeNode = null
    var parent: POCTreeNode = null
    var count: Int = -1
    var foreIndex: Int = -1
  }
}
