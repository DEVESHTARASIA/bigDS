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
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

class HybridFPGrowth (
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
    HybridFPGrowth.DEFAULT_SUPPORT_THRESHOLD,
    HybridFPGrowth.DEFAULT_SPLITTER_PATTERN,
    HybridFPGrowth.DEFAULT_NUM_GROUPS
  )

  def getOrCalcMinSupport(data: RDD[String]): Int = {
    if (minSupport < 0) minSupport = (data.count() * supportThreshold).toInt
    minSupport
  }

  def calcF1Items(data: RDD[String]): RDD[(String, Int)] = {
    val support = getOrCalcMinSupport(data)
/*    
    data.flatMap(arr => arr.split(splitterPattern).map((_, 1)))
        .reduceByKey(_ + _, HybridFPGrowth.DEFAULT_NUM_GROUPS)
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
//    .reduceByKey(_ + _, HybridFPGrowth.DEFAULT_NUM_GROUPS)
    .reduceByKey(_ + _)
    .filter(_._2 >= support)
  }

  def buildF1Map(f1List: Array[(String, Int)]): HashMap[String, Int] = {
    val f1Map = new HashMap[String, Int]()
    f1List.map { case (item, cnt) => item}
      .zipWithIndex
      .map { case (item, id) => f1Map(item) = id}
    f1Map
  }

  def getOrCreateChild(item: Int, parent: POCNode): POCNode = {

    // create children HashMap if it does not exist
    if (parent.children == null) parent.children = new HashMap[Int, POCNode]()

    var child = parent.children.getOrElse(item, null)
    if (child==null) {
      child = new POCNode(item, 0, parent)
      parent.children.update(item, child)
    }

    child
/*
    if (parent.child==null) { parent.child = new POCNode(item, 1, parent) }
    else {
      var sibling = parent.child
      while (sibling.item < item) sibling = sibling.sibling
    }
*/
  }

  def genPrefixTree(iterator: Iterator[Array[Int]]) : Iterator[POCNode] = {
    val tree = new POCNode()

    while (iterator.hasNext) { // process the partition to build a prefix tree (with prefixTree as root)
      val arr = iterator.next()

      var node = tree
      arr.foreach { item =>
        node = getOrCreateChild(item, node)
        node.count += 1
      }
    }

    Iterator(tree)
  }

  def scanF2(node : POCNode, prefix : Array[Int], depth : Int, hashCount : HashMap[(Int, Int), Int]) : Unit = {

    if (node.children!=null) {
      node.children.foreach{ case(childItem, childNode) =>

        // concat (any_prefix_item, current_child_item) to form a F2 candidate
        for(i <- 0 until depth) {
          val key = (prefix(i), childItem)
          hashCount.update(key, hashCount.getOrElse(key, 0) + childNode.count)
        }

        // recursively scan child node
        prefix(depth) = childItem
        scanF2(childNode, prefix, depth + 1, hashCount)
      }
    }
  }

  def scanFi(node : POCNode, prefix : Array[Array[Array[Int]]], depth : Int, f2Set : HashSet[(Int, Int)], hashCount : HashMap[String, Int]) : Unit = {
    println(s"Current in depth = " + depth)
    if (node.children!=null) {
      node.children.foreach{ case(childItem, childNode) =>
        val selfCand = new Array[Int](1)
        selfCand(0) = childItem
        val curFi = 
          prefix.take(depth).flatMap{ pi =>
            pi.filter{ arrOfFi =>
              var i = 0
              while((i < arrOfFi.length) && (f2Set.contains((arrOfFi(i), childItem)))) i += 1
              i==arrOfFi.length
            }
          }
          .map{ arrOfFi =>
            val arrOfCurFi = arrOfFi:+(childItem)
            if (arrOfCurFi.length>2) {
              val strOfCurFi = arrOfCurFi.mkString(splitterPattern)
              hashCount.update(strOfCurFi, hashCount.getOrElse(strOfCurFi, 0) + childNode.count)
            }
            arrOfCurFi
          }:+(selfCand)
        prefix(depth) = curFi
        scanFi(childNode, prefix, depth + 1, f2Set, hashCount)
      }
    }
  }

  def calcFiItemsets(forest : RDD[POCNode], f2Set : HashSet[(Int, Int)]) : RDD[(String, Int)] = {
    forest.flatMap{ case(root) =>
      val prefix = new Array[Array[Array[Int]]](numF1Items)
      val hashCount = new HashMap[String, Int]()

      val topPrefix = new Array[Array[Int]](1)
      topPrefix(0) = new Array[Int](1)
      prefix(0) = topPrefix
      root.children.foreach{ case(item, child) =>
        topPrefix(0)(0) = item
        scanFi(child, prefix, 1, f2Set, hashCount)
      }
      hashCount.iterator
    }
    .reduceByKey(_ + _)
    .filter(_._2 >= minSupport)
  }

  def calcF2Itemsets(forest : RDD[POCNode]) : RDD[((Int, Int), Int)] = {
/*  
    forest.flatMap{ case(root, hashCount) =>
      val trie = new HashMap[Int, Int]()
      hashCount.filter{ case(node, cnt) =>
        node.parent.item >= 0
      }.foreach{ case(node, cnt) =>
        val key = (node.parent.item << 16) & node.item
        trie.update(key, trie.getOrElse(key, 0) + cnt)
      }

      trie.toSeq
    }
    .reduceByKey(_ + _, HybridFPGrowth.DEFAULT_NUM_GROUPS)
//
*/
    forest.flatMap{ case(root) =>
      val prefix = new Array[Int](numF1Items)
      val hashCount = new HashMap[(Int, Int), Int]()
      root.children.foreach{ case(item, child) =>
        prefix(0) = item
        scanF2(child, prefix, 1, hashCount)
      }
      hashCount.iterator
    }
    .reduceByKey(_ + _)
    .filter(_._2 >= minSupport)
  }

  def buildF2Map(f2Itemsets : RDD[((Int, Int), Int)]) : HashMap[(Int, Int), Int] = {
    val f2Map = new HashMap[(Int, Int), Int]()
    f2Itemsets.collect.foreach { case(l, cnt) => f2Map(l) = cnt }
    f2Map
  }

  def buildF2Set(f2Itemsets : RDD[((Int, Int), Int)]) : HashSet[(Int, Int)] = {
    var f2Set = new HashSet[(Int, Int)]()
    f2Set++(f2Itemsets.map(_._1).collect)
  }

  /** Implementation of HybridFPGrowth. */
  def run(data: RDD[String]): RDD[(String, Int)] = {

    val sc = data.sparkContext
    val cdata = data.coalesce(HybridFPGrowth.DEFAULT_NUM_GROUPS).cache()

    // build f1list and f1map
    val f1List = calcF1Items(cdata)
    val bcF1List = sc.broadcast(f1List.collect().sortWith(_._2 > _._2))
    numF1Items = bcF1List.value.length
    println(s"f1List length = " + bcF1List.value.length + ", [" + bcF1List.value.mkString + "]")

    val f1Map = buildF1Map(bcF1List.value)
    val bcF1Map = sc.broadcast(f1Map)
//    println(s"f1Map length = " + f1Map.size + ", [" + f1Map.toArray.mkString + "]")

    // mining frequent item sets
    val prefixForest = cdata.map { record =>
      record.split(splitterPattern)
        .filter{ item => bcF1Map.value.contains(item)}
        .map(bcF1Map.value(_))
        .sortWith(_ < _)
    }
    .mapPartitions(genPrefixTree)
    .persist(StorageLevel.MEMORY_AND_DISK)

    cdata.unpersist()

    val f2Itemsets = calcF2Itemsets(prefixForest)
    val f2Set = buildF2Set(f2Itemsets)
    val bcF2Itemsets = sc.broadcast(f2Set)

/*
    println("Number of f1 itemsets: =" + bcF1List.value.length)
    println("Number of f2 itemsets: =" + f2Itemsets.count)
*/
    calcFiItemsets(prefixForest, bcF2Itemsets.value)
    .map{ case(strInternalFi, cnt) =>
      val strFi =
        strInternalFi.split(splitterPattern)
          .map{ strInt => bcF1List.value(strInt.toInt)._1 }
          .mkString(splitterPattern)

        (strFi, cnt)
    }
    .++(
      f2Itemsets.map{ case(key, cnt) =>
//      ((key >> 16).toString + s"," + (key & 65535).toString, cnt)
        (bcF1List.value(key._1)._1 + splitterPattern + bcF1List.value(key._2)._1, cnt)
      }.++(f1List))
  }
}

/**
 * Top-level methods for calling HybridFPGrowth.
 */
object HybridFPGrowth {

  // Default values.
  val DEFAULT_SUPPORT_THRESHOLD = 0
  val DEFAULT_SPLITTER_PATTERN = " "
  val DEFAULT_NUM_GROUPS = 256

  /**
   * Run HybridFPGrowth using the given set of parameters.
   * @param data transactional dataset stored as `RDD[String]`
   * @param supportThreshold support threshold
   * @param splitterPattern splitter pattern
   * @return frequent itemsets stored as `RDD[(String, Int)]`
   */
  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String): RDD[(String, Int)] = {
    new HybridFPGrowth()
      .setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .run(data)
  }

  def run(data: RDD[String], supportThreshold: Double): RDD[(String, Int)] = {
    new HybridFPGrowth()
      .setSupportThreshold(supportThreshold)
      .run(data)
  }

  def run(data: RDD[String]): RDD[(String, Int)] = {
    new HybridFPGrowth()
      .run(data)
  }
}
