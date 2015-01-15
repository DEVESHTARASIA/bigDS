/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bigds.mining.association.PFP

import org.apache.bigds.mining.association.FrequentItemsetMining
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.{ArrayBuffer, HashMap}

class DistFPGrowth (
    private var supportThreshold: Double,
    private var splitterPattern: String,
    private var numGroups: Int) extends FrequentItemsetMining {

  private var numF1Items = -1
  private var minSupport = -1

  /** Set the support threshold. Support threshold must be defined on interval [0, 1]. Default: 0. */
  def setSupportThreshold(supportThreshold: Double): this.type = {
    if (supportThreshold < 0 || supportThreshold > 1) {
      throw new IllegalArgumentException("Support threshold must be defined on interval [0, 1]")
    }
    this.supportThreshold = supportThreshold
    this
  }

  /** Set the splitter pattern within which we can split transactions into items. */
  def setSplitterPattern(splitterPattern: String): this.type = {
    this.splitterPattern = splitterPattern
    this
  }

  def this() = this(
    DistFPGrowth.DEFAULT_SUPPORT_THRESHOLD,
    DistFPGrowth.DEFAULT_SPLITTER_PATTERN,
    DistFPGrowth.DEFAULT_NUM_GROUPS
  )

  def getOrCalcMinSupport(data: RDD[String]): Int = {
    if (minSupport < 0) minSupport = (data.count() * supportThreshold).toInt
    minSupport
  }

  /** Set the number of groups the features should be divided. */
  def setNumGroups(numGroups: Int): this.type = {
    this.numGroups = numGroups
    this
  }

  def getGroupScopes(numItems: Int): Array[Int] = {
    var curItem = 0
    val avgItems = numItems / numGroups
    val residual = numItems % numGroups

    Range(0, numGroups - 1).map { gid =>
      curItem += avgItems
      if (gid < residual) {
        curItem += 1
      }
      curItem
    }.toArray
  }

  def calcF1Items(data: RDD[String]): Array[(String, Int)] = {
    val support = getOrCalcMinSupport(data)
    data.flatMap(arr => arr.split(splitterPattern).map((_, 1))).reduceByKey(_ + _).filter(_._2 >= support).sortBy(_._2, false).collect()
  }

  def buildF1Map(f1List: Array[(String, Int)]): HashMap[String, Int] = {
    val f1Map = new HashMap[String, Int]()
    f1List.map { case (item, cnt) => item}
      .zipWithIndex
      .map { case (item, id) => f1Map(item) = id}
    f1Map
  }

  def createNewItemNode(item: Int, parent: ItemNode, headTable: Array[ArrayBuffer[ItemNode]]): ItemNode = {
    val child = new ItemNode(item, parent)
    headTable(item) += child
    child
  }

  def genGroupedPrefixTree(iterator: Iterator[Array[Int]]): Iterator[(Int, (Int, Array[IndexNode]))] = {
    val prefixTree = new ItemNode()
    val headTable = new Array[ArrayBuffer[ItemNode]](numF1Items)
    val hashCount = new HashMap[ItemNode, Int]()

    var node: ItemNode = null
    iterator.map { arr => // process the partition to build a prefix tree (with prefixTree as root)
      node = prefixTree
      arr.map { item =>
        if (node.children == null) node.children = new HashMap[Int, ItemNode]()
        node = node.children.getOrElseUpdate(item, createNewItemNode(item, node, headTable))
        hashCount.put(node, hashCount.getOrElse(node, 0) + 1)
      }
    }

    var startx = 0
    getGroupScopes(numF1Items).zipWithIndex.map { case (scope, groupId) =>
      val nodeToCountOrIndex = new HashMap[ItemNode, Int]()
      for (item <- startx until (startx + scope)) {
        // hash all potential up-able entries
        headTable(item).map { node => nodeToCountOrIndex.put(node, hashCount(node))}
      }

      nodeToCountOrIndex.keys.map { bottom => // check each potential entry for going up (till root)
        var up = bottom.parent
        val cnt = hashCount(bottom)
        while (up != null && up.item < startx) {
          // Yes, it is a new entry to non-group nodes
          nodeToCountOrIndex.put(up, nodeToCountOrIndex.getOrElse(up, 0) + cnt)
          up = up.parent
        }
      }

      startx += scope

      // generate tree nodes for serialization, meanwhile change map (node->cnt) to (node->index)
      var i = 0
      var rootIndex = -1
      val serialTree = nodeToCountOrIndex.toArray.map { case (node, cnt) =>
        nodeToCountOrIndex.update(node, i)
        if (node == prefixTree) rootIndex = i
        i += 1
        new IndexNode(node.item, cnt)
      }

      // set up child and sibling indices for each serialized node
      nodeToCountOrIndex.map { case (node, nodeIndex) =>
        if (nodeIndex != rootIndex) {
          val p = serialTree(nodeToCountOrIndex(node.parent))
          if (p.child < 0) p.child = nodeIndex
          else {
            serialTree(nodeIndex).sibling = p.child
            p.child = nodeIndex
          }
        }
      }

      (groupId, (rootIndex, serialTree))
    }.toIterator
  }

  def itemFPGrowth(
       item: Int,
       postfix: Array[Int],
       prefixTree: ItemNode,
       headTable: Array[ArrayBuffer[ItemNode]],
       hashCount: HashMap[ItemNode, Int]): Array[(Array[Int], Int)] = {

    val nextHeadTable = new Array[ArrayBuffer[ItemNode]](item)
    val nextHashCount = new HashMap[ItemNode, Int]()

    var itemCnt = 0
    headTable(item).map { bottom =>
      var node = bottom
      val cnt = hashCount(node)
      itemCnt += cnt

      node = node.parent
      while (node != prefixTree) {
        val orgCnt = nextHashCount.getOrElse(node, 0)
        if (orgCnt == 0) nextHeadTable(node.item) += node
        nextHashCount.put(node, orgCnt + cnt)
        node = node.parent
      }
    }

    val nextPostfix = new Array[Int](postfix.length + 1)
    nextPostfix(0) = item
    if (postfix.length > 0) postfix.copyToArray(nextPostfix, 1)

    nextHeadTable.map(_.map(nextHashCount(_)).sum)
      .zipWithIndex
      .filter { case (sumCnt, nextItem) => sumCnt >= minSupport}
      .flatMap { case (sumCnt, nextItem) => itemFPGrowth(nextItem, nextPostfix, prefixTree, nextHeadTable, nextHashCount)} :+(postfix, itemCnt)
  }

  def genGroupFreqItemsets(groupId: Int, iterator: Iterable[(Int, Array[IndexNode])]): Array[(Array[Int], Int)] = {
    val prefixTree = new ItemNode()
    val headTable = new Array[ArrayBuffer[ItemNode]](numF1Items)
    val hashCount = new HashMap[ItemNode, Int]()

    def combineSerializedTree(curNode: ItemNode, curIndex: Int, indexTree: Array[IndexNode]): Unit = {
      var node = curNode
      var siblingIndex = indexTree(curIndex).child
      while (siblingIndex > 0) {
        val siblingIndexNode = indexTree(siblingIndex)
        if (node.children == null) node.children = new HashMap[Int, ItemNode]()
        node = node.children.getOrElseUpdate(siblingIndexNode.item, createNewItemNode(siblingIndexNode.item, node, headTable))
        hashCount.put(node, hashCount.getOrElse(node, 0) + siblingIndexNode.count)
        combineSerializedTree(node, siblingIndex, indexTree)
        siblingIndex = siblingIndexNode.sibling
      }
    }

    iterator.map { case (rootIndex, indexTree) => combineSerializedTree(prefixTree, rootIndex, indexTree)}

    val scope = getGroupScopes(numF1Items)
    val startx = scope(groupId)
    val endx = if (groupId == numGroups - 1) numF1Items else scope(groupId + 1)
    Range(startx, endx).flatMap { i =>
      itemFPGrowth(i, new Array[Int](0), prefixTree, headTable, hashCount)
    }.toArray
  }


  /** Implementation of DistFPGrowth. */
  def run(data: RDD[String]): RDD[(String, Int)] = {

    val sc = data.sparkContext

    // build f1list and f1map
    val f1List = calcF1Items(data)
    val f1Map = buildF1Map(f1List)

    numF1Items = f1List.length
    val bcF1List = sc.broadcast(f1List)
    val bcF1Map = sc.broadcast(f1Map)

    // mining frequent item sets
    data.map(_.split(splitterPattern).filter{ item => bcF1Map.value.contains(item) }.map(bcF1Map.value(_)).sortWith(_ < _))
      .mapPartitions {partition => genGroupedPrefixTree(partition) }
      .groupByKey()
      .flatMap { case (gid, serialTrees) => genGroupFreqItemsets(gid, serialTrees)}
      .map { case (itemset, cnt) => (itemset.map(bcF1List.value(_)._1).mkString(" "), cnt) }
  }
}

/**
 * Top-level methods for calling DistFPGrowth.
 */
object DistFPGrowth {

  // Default values.
  val DEFAULT_SUPPORT_THRESHOLD = 0
  val DEFAULT_SPLITTER_PATTERN = " "
  val DEFAULT_NUM_GROUPS = 128

  /**
   * Run DistFPGrowth using the given set of parameters.
   * @param data transactional dataset stored as `RDD[String]`
   * @param supportThreshold support threshold
   * @param splitterPattern splitter pattern
   * @param numGroups Number of groups the features should be divided.
   * @return frequent itemsets stored as `RDD[(String, Int)]`
   */
  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String,
      numGroups: Int): RDD[(String, Int)] = {
    new DistFPGrowth()
      .setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .setNumGroups(numGroups)
      .run(data)
  }

  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String): RDD[(String, Int)] = {
    new DistFPGrowth()
      .setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .run(data)
  }

  def run(data: RDD[String], supportThreshold: Double): RDD[(String, Int)] = {
    new DistFPGrowth()
      .setSupportThreshold(supportThreshold)
      .run(data)
  }

  def run(data: RDD[String]): RDD[(String, Int)] = {
    new DistFPGrowth()
      .run(data)
  }
}
