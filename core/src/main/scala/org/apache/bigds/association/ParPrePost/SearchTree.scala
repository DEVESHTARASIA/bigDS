import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform._
import scala.reflect.ClassTag

/**
 * Created by clin3 on 2015/6/4.
 */
class SearchTree[Item: ClassTag](ppctree: PPCTree[Item]) extends Serializable {
  import SearchTree._
  val root = new SearchTreeNode
  root.label = ppctree.numFreqItems
  root.firstChild = null
  val equivalentItems = new Array[Int](ppctree.numFreqItems)
  var resultCount: Int = 0
  var nodeListLenSum: Int = 0
  var resultLen: Int = 0
  val result = new Array[Int](ppctree.numFreqItems)
  var nodeListCount: Int = 0
  val freqItemsets: ArrayBuffer[(Array[Item], Long)] = ArrayBuffer.empty[(Array[Item], Long)]
  var numFreqItemsets: Int = 0

  def initialize[Item: ClassTag](itemToRank: Array[(Item, Long)]): Unit = {
    var lastChild: SearchTreeNode = null
    for(i <- (ppctree.numFreqItems - 1) to 0 by -1) {
      if(ppctree.bfCursor > ppctree.bfCurrentSize - ppctree.headTableLen(i) * 3) {
        ppctree.bfRowIndex += 1
        ppctree.bfCursor = 0
        ppctree.bfCurrentSize = 10 * ppctree.bfSize
        ppctree.bf(ppctree.bfRowIndex) = new Array[Int](ppctree.bfCurrentSize)
      }

      val patternTreeNode = new SearchTreeNode
      ppctree.nodelistCount += 1
      patternTreeNode.label = i
      patternTreeNode.support = 0
      patternTreeNode.beginInBf = ppctree.bfCursor
      patternTreeNode.length = 0
      patternTreeNode.rowIndex = ppctree.bfRowIndex
      var ni = ppctree.headTable(i)
      while(ni != null) {
        patternTreeNode.support += ni.count
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ni.foreIndex
        ppctree.bfCursor += 1
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ni.backIndex
        ppctree.bfCursor += 1
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ni.count
        ppctree.bfCursor += 1
        ppctree.bfCount += 3
        patternTreeNode.length += 1
        ni = ni.labelSibling
      }
      if(root.firstChild == null) {
        root.firstChild = patternTreeNode
        lastChild = patternTreeNode
      } else {
        lastChild.next = patternTreeNode
        lastChild = patternTreeNode
      }
    }
  }

  def mine(
      fromCursor: Int,
      fromRowIndex: Int,
      fromSize: Int,
      id: Int,
      partitioner: Partitioner): ArrayBuffer[(Array[Item], Long)] = {
    var curNode = root.firstChild
    var next: SearchTreeNode = null
    while(curNode != null) {
      next = curNode.next
      if(partitioner.getPartition(curNode.label) == id) {
        traverse(curNode, this.root, 1, 0)
        for (i <- ppctree.bfRowIndex until fromRowIndex by -1) {
          ppctree.bf(i) = null
        }
        ppctree.bfRowIndex = fromRowIndex
        ppctree.bfCursor = fromCursor
        ppctree.bfCurrentSize = fromSize
      }
      curNode = next
    }
    this.freqItemsets
  }

  def traverse(
      curNode: SearchTreeNode,
      curRoot: SearchTreeNode,
      level: Int,
      count: Int): Unit = {
    var sibling = curNode.next
    var equivalentCount = count
    var lastChild: SearchTreeNode = null
    var startTime = 0L
    if (level == 1) {
      startTime = currentTime
    }

    while(sibling != null) {
      if(level > 1 || (level == 1 && ppctree.itemsetCount((curNode.label - 1) * curNode.label / 2 + sibling.label) >= ppctree.minCount)) {
        val equivalentCountTemp = new IntegerByRef(equivalentCount)
        equivalentCountTemp.count = equivalentCount
        lastChild = iskItemsetEquivalent(curNode, sibling, level, lastChild, equivalentCountTemp)
        equivalentCount = equivalentCountTemp.count
      }
      sibling = sibling.next
    }
    if (level == 1) {
      val endTime = currentTime
      val totalTime: Double = endTime - startTime
      ppctree.level1 += (totalTime / 1000)
    }
    this.resultCount +=  Math.pow(2.0, equivalentCount.toDouble).toInt
    this.nodeListLenSum += Math.pow(2.0, equivalentCount.toDouble).toInt * curNode.length
    this.result(this.resultLen) = curNode.label
    this.resultLen += 1
    genFreqItemsets(curNode, equivalentCount)


    this.nodeListCount += 1
    val fromCursor = ppctree.bfCursor
    val fromRowIndex = ppctree.bfRowIndex
    val fromSize = ppctree.bfCurrentSize
    var child: SearchTreeNode = curNode.firstChild
    var next: SearchTreeNode = null
    while(child != null) {
      next = child.next
      traverse(child, curNode, level + 1, equivalentCount)
      for(i <- ppctree.bfRowIndex until fromRowIndex by -1) {
        ppctree.bf(i) = null
      }
      ppctree.bfRowIndex = fromRowIndex
      ppctree.bfCursor = fromCursor
      ppctree.bfCurrentSize = fromSize
      child = next
    }
    this.resultLen -= 1
  }

  def iskItemsetEquivalent(
      ni: SearchTreeNode,
      nj: SearchTreeNode,
      level: Int,
      node: SearchTreeNode,
      equivalentCount: IntegerByRef): SearchTreeNode = {
    var lastChild = node
    if(ppctree.bfCursor + ni.length * 3 > ppctree.bfCurrentSize) {
      ppctree.bfRowIndex += 1
      ppctree.bfCursor = 0
      ppctree.bfCurrentSize = if(ppctree.bfSize > ni.length * 1000) ppctree.bfSize else ni.length * 1000
      ppctree.bf(ppctree.bfRowIndex) = new Array[Int](ppctree.bfCurrentSize)
    }

    var patterntreeNode = new SearchTreeNode
    ppctree.nodelistCount += 1
    patterntreeNode.beginInBf= ppctree.bfCursor
    patterntreeNode.rowIndex = ppctree.bfRowIndex

    var cursorI = ni.beginInBf
    var cursorJ = nj.beginInBf
    val rowIndexI = ni.rowIndex
    val rowIndexJ = nj.rowIndex
    var lastCur = -1

    while(cursorI < (ni.beginInBf + ni.length * 3) && cursorJ < (nj.beginInBf + nj.length * 3)) {
      if(ppctree.bf(rowIndexI)(cursorI) > ppctree.bf(rowIndexJ)(cursorJ) && ppctree.bf(rowIndexI)(cursorI + 1) < ppctree.bf(rowIndexJ)(cursorJ + 1)) {
        if(lastCur == cursorJ) {
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor - 1) += ppctree.bf(rowIndexI)(cursorI + 2)
        }
        else {
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexJ)(cursorJ)
          ppctree.bfCursor += 1
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexJ)(cursorJ + 1)
          ppctree.bfCursor += 1
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexI)(cursorI + 2)
          ppctree.bfCursor += 1
          ppctree.bfCount += 3
          patterntreeNode.length += 1
        }
        patterntreeNode.support += ppctree.bf(rowIndexI)(cursorI + 2)
        lastCur = cursorJ
        cursorI += 3
      } else if(ppctree.bf(rowIndexI)(cursorI) < ppctree.bf(rowIndexJ)(cursorJ)) {
        cursorI += 3
      } else if(ppctree.bf(rowIndexI)(cursorI + 1) > ppctree.bf(rowIndexJ)(rowIndexJ + 1)) {
        cursorJ += 3
      }
    }
    if(patterntreeNode.support >= ppctree.minCount) {
      if(ni.support == patterntreeNode.support && patterntreeNode.length == 1) {
        this.equivalentItems(equivalentCount.count) = nj.label
        equivalentCount.count += 1
        ppctree.bfCursor = patterntreeNode.beginInBf
        if(patterntreeNode != null) {
          patterntreeNode = null
        }
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
      ppctree.bfCursor = patterntreeNode.beginInBf
      if(patterntreeNode != null) {
        patterntreeNode = null
      }
    }
    lastChild
  }

  def genFreqItemsets(curNode: SearchTreeNode, equivalentCount: Int): Unit ={
    if(curNode.support >= ppctree.minCount) {
      this.numFreqItemsets += 1
      val freqItemset = ArrayBuffer.empty[Item]
      for(i <- 0 until this.resultLen) {
        freqItemset += ppctree.rankToItem.get(this.result(i)).get
      }
      val tuple = (freqItemset.toArray, curNode.support)
      this.freqItemsets += tuple
    }
    if(equivalentCount > 0) {
      val max = 1 << equivalentCount
      for(i <- 1 until max) {
        val freqItemset = ArrayBuffer.empty[Item]
        for(k <- 0 until this.resultLen) {
          freqItemset += ppctree.rankToItem.get(this.result(k)).get
        }
        for(j <- 0 until equivalentCount) {
          val isSet = i & (1 << j)
          if(isSet > 0) {
            freqItemset += ppctree.rankToItem.get(this.equivalentItems(j)).get
          }
        }
        val tuple = (freqItemset.toArray, curNode.support)
        this.freqItemsets += tuple
        this.numFreqItemsets += 1
      }
    }
  }

  class IntegerByRef(var count:Int)
}

object SearchTree {
  class SearchTreeNode extends Serializable {
    var label: Int = _
    var firstChild: SearchTreeNode = null
    var next: SearchTreeNode = null
    var support: Long = 0
    var beginInBf: Int = -1
    var length: Int = 0
    var rowIndex: Int = 0
  }
}
