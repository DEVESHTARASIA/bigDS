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
package org.apache.bigds.mining.association.HybridFP

import org.apache.bigds.mining.association.FrequentItemsetMining
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
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

  def calcF1Items(data: RDD[String]): RDD[(String, Int)] = {
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
        val record = iter.next()
        record.split(splitterPattern).map{ s => hashItems.update(s, hashItems.getOrElse(s, 0) + 1) }
      }
      hashItems.iterator
    }
    .reduceByKey(_ + _, DistFPGrowth.DEFAULT_NUM_GROUPS)
    .filter(_._2 >= support)
  }

  def buildF1Map(f1List: Array[(String, Int)]): HashMap[String, Int] = {
    val f1Map = new HashMap[String, Int]()
    f1List.map { case (item, cnt) => item}
      .zipWithIndex
      .map { case (item, id) => f1Map(item) = id}
    f1Map
  }

  def getOrCreateChild(item: Int, parent: ItemNode): ItemNode = {

    // create children HashMap if it does not exist
    if (parent.children == null) parent.children = new HashMap[Int, ItemNode]()

    var child = parent.children.getOrElse(item, null)
    if (child==null) {
      child = new ItemNode(item, parent)
      parent.children.update(item, child)
    }

    child
  }

  def genPrefixTree(iterator: Iterator[Array[Int]]) : Iterator[PrefixTree] = {
    val tree = new ItemNode()
    val hashCount = new HashMap[ItemNode, Int]()

    while (iterator.hasNext) { // process the partition to build a prefix tree (with prefixTree as root)
    val arr = iterator.next()

      var node = tree
      arr.foreach { item =>
        node = getOrCreateChild(item, node)
        val cnt = hashCount.getOrElse(node, 0) + 1
        hashCount.update(node, cnt)
      }
    }
    val trees = new Array[PrefixTree](1)
    trees(0) = new PrefixTree(tree, hashCount)
    trees.iterator
  }

  def calcF2Itemsets(forest : RDD[PrefixTree]) : RDD[(Int, Int)] = {
    forest.flatMap{ tree =>
      val trie = new HashMap[Int, Int]()
      tree.hashCount.foreach{ case(node, cnt) =>
        val key = node.parent.item << 16 & node.item
        trie.update(key, trie.getOrElse(key, 0) + cnt)
      }

      trie.toArray
    }
    .reduceByKey(_ + _)
    .filter(_._2 > minSupport)
  }
/*
  def scanTree(node : ItemNode, candidates : HashMap[String, Int], ftable : Array[ArrayBuffer[Array[Int]]]) = {

  }

  def calcFiItemsets(forest : RDD[PrefixTree]) : RDD[(String, Int)] = {
    forest.flatMap{ tree =>
      val candidates = new HashMap[String, Int]()

      tree.root
      tree.headTable.zipWithIndex.foreach { case (homonyms, item) =>
        homonyms.foreach{ node =>
          node.children
        }
      }
      val tr
    }
  }
*/

  /** Implementation of DistFPGrowth. */
  def run(data: RDD[String]): RDD[(String, Int)] = {

    val sc = data.sparkContext

    // build f1list and f1map
    val f1List = calcF1Items(data)
    val bcF1List = sc.broadcast(f1List.collect().sortWith(_._2 > _._2))
    numF1Items = bcF1List.value.length
    println(s"f1List length = " + bcF1List.value.length + ", [" + bcF1List.value.mkString + "]")

    val f1Map = buildF1Map(bcF1List.value)
    val bcF1Map = sc.broadcast(f1Map)
//    println(s"f1Map length = " + f1Map.size + ", [" + f1Map.toArray.mkString + "]")

    // mining frequent item sets
    val prefixForest = data.map { record =>
      record.split(splitterPattern)
        .filter{ item => bcF1Map.value.contains(item)}
        .map(bcF1Map.value(_))
        .sortWith(_ < _)
    }
    .mapPartitions{ partition => genPrefixTree(partition) }
    .persist()

    println("groupByKey size: =" + prefixForest.count())

    val f2Itemsets = calcF2Itemsets(prefixForest)
    val bcF2Itemsets = sc.broadcast(f2Itemsets.collect())
/*
    val fiItemsets = calcFiItemsets(prefixForest)

    prefixForest.flatMap { tree => genGroupFreqItemsets(gid, serialTrees)}
     .map { case (itemset, cnt) => (itemset.map(bcF1List.value(_)._1).mkString(" "), cnt) }
  }
*/
    f2Itemsets.map{ case(key, cnt) =>
      ((key >> 16).toString + s"," + (key & 65535).toString, cnt)
    }.++(f1List)
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
