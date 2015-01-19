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
    val avgItems = numItems / numGroups
    val residual = numItems % numGroups

    if (numItems <= numGroups) Range(1, numItems+1).toArray
    else {
      var curItem = 0
      Range(0, numGroups).map { gid =>
        curItem += avgItems
        if (gid < residual) {
          curItem += 1
        }
        curItem
      }.toArray
    }
  }

  def calcF1Items(data: RDD[String]): Array[(String, Int)] = {
    val support = getOrCalcMinSupport(data)
/*    
    data.flatMap(arr => arr.split(splitterPattern).map((_, 1)))
        .reduceByKey(_ + _, DistFPGrowth.DEFAULT_NUM_GROUPS)
        .filter(_._2 >= support)
        .sortBy(_._2, false)
        .collect()
*/    
    data.mapPartitions{ iter =>
      val hashItems = new HashMap[String, Int]()
      while (iter.hasNext) {
        val record = iter.next
        record.split(splitterPattern).map{ s => hashItems.update(s, hashItems.getOrElse(s, 0) + 1) }
      }
      hashItems.iterator
    }
    .reduceByKey(_ + _, DistFPGrowth.DEFAULT_NUM_GROUPS)
    .filter(_._2 >= support)
    .collect()
    .sortWith(_._2 > _._2)
  }

  def buildF1Map(f1List: Array[(String, Int)]): HashMap[String, Int] = {
    val f1Map = new HashMap[String, Int]()
    f1List.map { case (item, cnt) => item}
      .zipWithIndex
      .map { case (item, id) => f1Map(item) = id}
    f1Map
  }

  def getOrCreateChild(item: Int, parent: ItemNode, headTable: Array[ArrayBuffer[ItemNode]]): ItemNode = {

    // create children HashMap if it does not exist
    if (parent.children == null) parent.children = new HashMap[Int, ItemNode]()

    var child = parent.children.getOrElse(item, null)
    if (child==null) {
      child = new ItemNode(item, parent)
      parent.children.update(item, child)
      headTable(item) += child
    }

    child
  }

  def genGroupedPrefixTree(iterator: Iterator[Array[Int]]): Iterator[(Int, (Int, Array[IndexNode]))] = {
    val prefixTree = new ItemNode()
    val headTable = new Array[ArrayBuffer[ItemNode]](numF1Items)
    for(i <- 0 until numF1Items) { headTable(i) = new ArrayBuffer[ItemNode]() }
    val hashCount = new HashMap[ItemNode, Int]()

//    println("iterator size =" + iterator.size)

    while (iterator.hasNext) { // process the partition to build a prefix tree (with prefixTree as root)
      val arr = iterator.next
      println(s"i am here!!!")
      println(s"record = [" + arr.mkString(",") + "]")

      var node = prefixTree
      arr.foreach { item =>
        node = getOrCreateChild(item, node, headTable)
        val cnt = hashCount.getOrElse(node, 0) + 1
        hashCount.update(node, cnt)
      }
    }

    println(s"headTable=" + headTable.map{ _.length }.mkString(","))
    println(s"hashCount=" + hashCount.toArray.map{ case(n, c) => "(" + n.item.toString + ", " + c + ")" }.mkString(" "))

    var beginx = 0
    getGroupScopes(numF1Items).zipWithIndex.map { case (scope, groupId) =>
      var numNoneZero = 0
      val prex = beginx
      for (item <- beginx until scope) {
        numNoneZero += headTable(item).length
      }

      beginx = scope
      (numNoneZero, prex, scope, groupId)
    }
    .filter(_._1>0)
    .map { case(numNoneZero, startx, scope, groupId) =>
      val nodeToCountOrIndex = new HashMap[ItemNode, Int]()
      for (item <- startx until scope) {
        // hash all potential up-able entries
        headTable(item).toArray.foreach { nd => nodeToCountOrIndex.update(nd, hashCount(nd))}
      }
//      println(s"nodeToCountOrIndex = " + nodeToCountOrIndex.toArray.map{ case(n, c) => "(" + n.item.toString + ", " + c + ")" }.mkString(" "))

      nodeToCountOrIndex.toArray.foreach { case (bottom, xc) => // check each potential entry for going up (till root)
        var up = bottom.parent
        var cnt = -1
        try {
          cnt = hashCount(bottom)
        } catch {
          case ex : NoSuchElementException => {
            println(s"startx = " + startx + ", scope = " + scope + ", node info: (" + bottom.item.toString + ")")
            println(s"headTable(item) = " + headTable(startx).toArray.map{ nd => "(" + nd.item.toString + ", " + hashCount(nd) + ")" }.mkString(" "))
            println(s"nodeToCountOrIndex = " + nodeToCountOrIndex.toArray.map{ case(n, c) => "(" + n.item.toString + ", " + c + ")" }.mkString(" "))
            throw ex
          }
        }
        while (up != null && up.item < startx) {
          // Yes, it is a new entry to non-group nodes
          nodeToCountOrIndex.update(up, nodeToCountOrIndex.getOrElse(up, 0) + cnt)
          up = up.parent
        }
      }

      // generate tree nodes for serialization, meanwhile change map (node->cnt) to (node->index)
      var i = 0
      var rootIndex = -1
      val serialTree = nodeToCountOrIndex.toArray.map { case (nd, cnt) =>
        nodeToCountOrIndex.update(nd, i)
        if (nd == prefixTree) rootIndex = i
        i += 1
        new IndexNode(nd.item, cnt)
      }

      // set up child and sibling indices for each serialized node
      nodeToCountOrIndex.foreach { case (node, nodeIndex) =>
        if (nodeIndex != rootIndex) {
          val p = serialTree(nodeToCountOrIndex(node.parent))
          if (p.child < 0) p.child = nodeIndex
          else {
            serialTree(nodeIndex).sibling = p.child
            p.child = nodeIndex
          }
        }
      }
//      println(s"nodeToCountOrIndex = " + nodeToCountOrIndex.toArray.map{ case(n, c) => "(" + n.item.toString + ", " + c + ")" }.mkString(" "))
//      println(s"groupId = " + groupId)
//      println(s"rootIndex = " + rootIndex)
      println(s"groupId = " + groupId + s", rootIndex = " + rootIndex + s", serialTree = " + serialTree.map(_.toString).mkString(" "))

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
    for(i <- 0 until item) { nextHeadTable(i) = new ArrayBuffer[ItemNode](0) }
    val nextHashCount = new HashMap[ItemNode, Int]()

    var itemCnt = 0
    headTable(item).toArray.map { bottom =>
      var node = bottom
      val cnt = hashCount(node)
      itemCnt += cnt

      node = node.parent
      while (node != prefixTree) {
        val orgCnt = nextHashCount.getOrElse(node, 0)
        try {
          if (orgCnt == 0) nextHeadTable(node.item) += node
        } catch {
          case ex : ArrayIndexOutOfBoundsException => println(s"<item = " + item + ", node.item = " +  node.item + ">")
          throw ex
        }
        nextHashCount.update(node, orgCnt + cnt)
        node = node.parent
      }
    }

    val nextPostfix = new Array[Int](postfix.length + 1)
    nextPostfix(0) = item
    if (postfix.length > 0) postfix.copyToArray(nextPostfix, 1)

    nextHeadTable.toArray.map(_.map(nextHashCount(_)).sum)
      .zipWithIndex
      .filter { case (sumCnt, nextItem) => sumCnt >= minSupport }
      .flatMap { case (sumCnt, nextItem) => itemFPGrowth(nextItem, nextPostfix, prefixTree, nextHeadTable, nextHashCount)} :+(nextPostfix, itemCnt)
  }

  def genGroupFreqItemsets(groupId: Int, iterator: Iterable[(Int, Array[IndexNode])]): Array[(Array[Int], Int)] = {
    val prefixTree = new ItemNode()
    val headTable = new Array[ArrayBuffer[ItemNode]](numF1Items)
    for(i <- 0 until numF1Items) headTable(i) = new ArrayBuffer[ItemNode]()
    val hashCount = new HashMap[ItemNode, Int]()

    // to recursively combine a serialized tree (i.e., Array[IndexNode]) into curNode
    def combineSerializedTree(curNode: ItemNode, curIndex: Int, indexTree: Array[IndexNode]): Unit = {

      // add count for current node
      hashCount.update(curNode, hashCount.getOrElse(curNode, 0) + indexTree(curIndex).count)

      // repeatedly process all child nodes
      var childIndex = indexTree(curIndex).child
      while (childIndex > 0) {

        val childIndexNode = indexTree(childIndex)

        val childNode = getOrCreateChild(childIndexNode.item, curNode, headTable)

        // recursively process one child
        combineSerializedTree(childNode, childIndex, indexTree)

        childIndex = childIndexNode.sibling
      }
    }

    iterator.foreach { case (rootIndex, indexTree) =>
//      println(s"root - " + rootIndex + ", " + indexTree.map(_.printInfo()).mkString(" "))
      combineSerializedTree(prefixTree, rootIndex, indexTree)
    }

    val scope = getGroupScopes(numF1Items)
    val startx = if (groupId==0) 0 else scope(groupId-1)
    val endx = scope(groupId)
    Range(startx, endx).flatMap { i =>
      itemFPGrowth(i, new Array[Int](0), prefixTree, headTable, hashCount)
    }.toArray
  }


  /** Implementation of DistFPGrowth. */
  def run(data: RDD[String]): RDD[(String, Int)] = {

    val sc = data.sparkContext

    // build f1list and f1map
    val f1List = calcF1Items(data)
    println(s"f1List length = " + f1List.length + ", [" + f1List.mkString + "]")
    val f1Map = buildF1Map(f1List)
    println(s"f1Map length = " + f1Map.size + ", [" + f1Map.toArray.mkString + "]")

    numF1Items = f1List.length
    val bcF1List = sc.broadcast(f1List)
    val bcF1Map = sc.broadcast(f1Map)

    // mining frequent item sets
    val g = data.map(_.split(splitterPattern).filter{ item => bcF1Map.value.contains(item) }.map(bcF1Map.value(_)).sortWith(_ < _))
/*    
    val f = e.collect
    println(s"data size = " + f.length)
    f.map{ r => println("[" + r.mkString(",") + "]") }

    val g = e
*/
      .mapPartitions { partition => genGroupedPrefixTree(partition) }
      .groupByKey().persist()
    println("groupByKey size: =" + g.count())
/*
    println("groupByKey size: =" + g.count)

    g.map{ case(gid, serialTrees) =>
      serialTrees.toArray.map{ case(rid, arr) =>
        "(" + gid + ", (" + rid + ", " + arr.map(_.printInfo()).mkString(" ") + "))"
      }
    }
    .collect
    .map(println)
*/
    g.flatMap { case (gid, serialTrees) => genGroupFreqItemsets(gid, serialTrees)}
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
  val DEFAULT_NUM_GROUPS = 192

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
