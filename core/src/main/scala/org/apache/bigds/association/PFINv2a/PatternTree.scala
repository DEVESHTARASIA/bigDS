package org.apache.spark.armlib.fpm

import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class PatternTree[Item: ClassTag](poctree: POCTree[Item]) extends Serializable {
  import org.apache.spark.armlib.fpm.PatternTree._
  val root = new PatternTreeNode
  root.label = poctree.numFreqItems
  root.firstChild = null
  val equivalentItems = new Array[Int](poctree.numFreqItems)
  var resultCount: Int = 0
  var nodesetLenSum: Int = 0
  var resultLen: Int = 0
  val result = new Array[Int](poctree.numFreqItems)
  var nodesetCount: Int = 0
  val freqItemsets: ArrayBuffer[(Array[Item], Long)] = ArrayBuffer.empty[(Array[Item], Long)]
  var numFreqItemsets: Int = 0

  def initialize[Item: ClassTag](itemToRank: Array[(Item, Long)]): Unit = {
    var lastChild: PatternTreeNode = null
    for(i <- (poctree.numFreqItems - 1) to 0 by -1) {
      val patternTreeNode = new PatternTreeNode
      patternTreeNode.label = i
      patternTreeNode.support = 0
      patternTreeNode.beginInBf = poctree.bfCursor
      patternTreeNode.length = 0
      patternTreeNode.rowIndex = poctree.bfRowIndex
      patternTreeNode.support = itemToRank(i)._2
      if(root.firstChild == null) {
        root.firstChild = patternTreeNode
        lastChild = patternTreeNode
      } else {
        lastChild.next = patternTreeNode
        lastChild = patternTreeNode
      }
    }
  }

  def mine(fromCursor: Int, fromRowIndex: Int, fromSize: Int, id: Int, partitioner: Partitioner): ArrayBuffer[(Array[Item], Long)] = {
    var curNode = root.firstChild
    var next: PatternTreeNode = null
    while(curNode != null) {
      next = curNode.next
      if(partitioner.getPartition(curNode.label) == id) {
        traverse(curNode, this.root, 1, 0)
        for (i <- poctree.bfRowIndex until fromRowIndex by -1) {
          poctree.bf(i) = null
        }
        poctree.bfRowIndex = fromRowIndex
        poctree.bfCursor = fromCursor
        poctree.bfCurrentSize = fromSize
      }
      curNode = next
    }
    this.freqItemsets
  }

  /**
   * Recursively traverse the pattern tree to find frequent itemsets
   */
  def traverse(
      curNode: PatternTreeNode,
      curRoot: PatternTreeNode,
      level: Int,
      count: Int): Unit = {
    var sibling = curNode.next
    var equivalentCount = count
    var lastChild: PatternTreeNode = null

    while(sibling != null) {
      if((level == 1 && poctree.itemsetCount((curNode.label - 1) * curNode.label / 2 + sibling.label) >= poctree.minCount)) {
        val equivalentCountTemp = new IntegerByRef(equivalentCount)
        lastChild = is2ItemsetEquivalent(curNode, sibling, level, lastChild, equivalentCountTemp)
        equivalentCount = equivalentCountTemp.count
      } else if(level > 1) {
        val equivalentCountTemp = new IntegerByRef(equivalentCount)
        lastChild = iskItemsetEquivalent(curNode, sibling, level, lastChild, equivalentCountTemp)
        equivalentCount = equivalentCountTemp.count
      }
      sibling = sibling.next
    }
    this.resultCount +=  Math.pow(2.0, equivalentCount.toDouble).toInt
    this.nodesetLenSum += Math.pow(2.0, equivalentCount.toDouble).toInt * curNode.length
    this.result(this.resultLen) = curNode.label
    this.resultLen += 1
    genFreqItemsets(curNode, equivalentCount)
    this.nodesetCount += 1
    val fromCursor = poctree.bfCursor
    val fromRowIndex = poctree.bfRowIndex
    val fromSize = poctree.bfCurrentSize
    var child: PatternTreeNode = curNode.firstChild
    var next: PatternTreeNode = null
    while(child != null) {
      next = child.next
      traverse(child, curNode, level + 1, equivalentCount)
      for(i <- poctree.bfRowIndex until fromRowIndex by -1) {
        poctree.bf(i) = null
      }
      poctree.bfRowIndex = fromRowIndex
      poctree.bfCursor = fromCursor
      poctree.bfCurrentSize = fromSize
      child = next
    }
    this.resultLen -= 1
  }

  class IntegerByRef(var count:Int)

  def is2ItemsetEquivalent(
      ni: PatternTreeNode,
      nj: PatternTreeNode,
      level: Int,
      node: PatternTreeNode,
      equivalentCount: IntegerByRef): PatternTreeNode ={
    var lastChild = node
    val i = ni.label
    val j = nj.label
    if(ni.support == poctree.itemsetCount((i - 1) * i / 2 + j)) {
      this.equivalentItems(equivalentCount.count) = nj.label
      equivalentCount.count += 1
    } else {
      val patterntreeNode = new PatternTreeNode
      patterntreeNode.label = j
      patterntreeNode.rowIndex = poctree.nodesetRowIndex
      patterntreeNode.beginInBf = poctree.nodesetBegin((i - 1) * i / 2 + j)
      patterntreeNode.length = poctree.nodesetLen((i - 1) * i / 2 + j)
      patterntreeNode.support = poctree.itemsetCount((i - 1) * i / 2 + j)
      if(ni.firstChild == null) {
        ni.firstChild = patterntreeNode
        lastChild = patterntreeNode
      } else {
        lastChild.next = patterntreeNode
        lastChild = patterntreeNode
      }
    }
    lastChild
  }

  def iskItemsetEquivalent(
      ni: PatternTreeNode,
      nj: PatternTreeNode,
      level: Int,
      node: PatternTreeNode,
      equivalentCount: IntegerByRef): PatternTreeNode = {
    var lastChild = node
    if(poctree.bfCursor + ni.length > poctree.bfCurrentSize) {
      poctree.bfRowIndex += 1
      poctree.bfCursor = 0
      poctree.bfCurrentSize = if(poctree.bfSize > ni.length * 1000) poctree.bfSize else ni.length * 1000
      poctree.bf(poctree.bfRowIndex) = new Array[Int](poctree.bfCurrentSize)
    }

    val patterntreeNode = new PatternTreeNode
    patterntreeNode.beginInBf = poctree.bfCursor
    patterntreeNode.rowIndex = poctree.bfRowIndex

    var cursorI = ni.beginInBf
    var cursorJ = nj.beginInBf
    val rowIndexI = ni.rowIndex
    val rowIndexJ = nj.rowIndex
    while(cursorI < (ni.beginInBf + ni.length) && cursorJ < (nj.beginInBf + nj.length)) {
      if(poctree.bf(rowIndexI)(cursorI) == poctree.bf(rowIndexJ)(cursorJ)) {
        poctree.bf(poctree.bfRowIndex)(poctree.bfCursor) = poctree.bf(rowIndexJ)(cursorJ)
        poctree.bfCursor += 1
        patterntreeNode.length += 1
        val pos = poctree.bf(rowIndexI)(cursorI)
        patterntreeNode.support += poctree.supportDict(pos)
        cursorI += 1
        cursorJ += 1
      } else if (poctree.bf(rowIndexI)(cursorI) < poctree.bf(rowIndexJ)(cursorJ)) {
        cursorI += 1
      } else {
        cursorJ += 1
      }
    }
    if(patterntreeNode.support >= poctree.minCount) {
      if(ni.support == patterntreeNode.support) {
        this.equivalentItems(equivalentCount.count) = nj.label
        equivalentCount.count += 1
      } else {
        patterntreeNode.label = nj.label
        patterntreeNode.firstChild = null
        patterntreeNode.next = null
        if(ni.firstChild == null) {
          ni.firstChild = patterntreeNode
          lastChild = patterntreeNode
        } else {
          lastChild.next = patterntreeNode
          lastChild = patterntreeNode
        }
      }
    } else {
      poctree.bfCursor = patterntreeNode.beginInBf
    }
    lastChild
  }

  def genFreqItemsets(curNode: PatternTreeNode, equivalentCount: Int): Unit ={
    if(curNode.support >= poctree.minCount) {
      this.numFreqItemsets += 1
      val freqItemset = ArrayBuffer.empty[Item]
      for(i <- 0 until this.resultLen) {
        freqItemset += poctree.rankToItem.get(this.result(i)).get
      }
      val tuple = (freqItemset.toArray, curNode.support)
      this.freqItemsets += tuple
    }
    if(equivalentCount > 0) {
      val max = 1 << equivalentCount
      for(i <- 1 until max) {
        val freqItemset = ArrayBuffer.empty[Item]
        for(k <- 0 until this.resultLen) {
          freqItemset += poctree.rankToItem.get(this.result(k)).get
        }
        for(j <- 0 until equivalentCount) {
          val isSet = i & (1 << j)
          if(isSet > 0) {
            freqItemset += poctree.rankToItem.get(this.equivalentItems(j)).get
          }
        }
        val tuple = (freqItemset.toArray, curNode.support)
        this.freqItemsets += tuple
        this.numFreqItemsets += 1
      }
    }
  }
}

object PatternTree {
  class PatternTreeNode extends Serializable {
    var label: Int = _
    var firstChild: PatternTreeNode = null
    var next: PatternTreeNode = null
    var support: Long = 0
    var beginInBf: Int = -1
    var length: Int = 0
    var rowIndex: Int = 0
  }
}
