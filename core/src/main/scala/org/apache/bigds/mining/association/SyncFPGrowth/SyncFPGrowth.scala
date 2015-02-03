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
package org.apache.bigds.mining.association.SyncFPGrowth

import org.apache.bigds.mining.association.FrequentItemsetMining
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

class SyncFPGrowth (
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
    SyncFPGrowth.DEFAULT_SUPPORT_THRESHOLD,
    SyncFPGrowth.DEFAULT_SPLITTER_PATTERN,
    SyncFPGrowth.DEFAULT_NUM_GROUPS
  )

  def getOrCalcMinSupport(data: RDD[String]): Int = {
    if (minSupport < 0) minSupport = (supportThreshold * data.count()).ceil.toInt
//    if (minSupport < 0) minSupport = (data.count() * supportThreshold).toInt
    minSupport
  }

  def calcF1Items(data: RDD[String]): RDD[(String, Int)] = {
    val support = getOrCalcMinSupport(data)
/*
    data.flatMap(arr => arr.split(splitterPattern).map((_, 1)))
        .reduceByKey(_ + _, SyncFPGrowth.DEFAULT_NUM_GROUPS)
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
//    .reduceByKey(_ + _, SyncFPGrowth.DEFAULT_NUM_GROUPS)
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

  def getOrCreateChild(
       item: Int,
       parent: POCNode,
       headTable: Array[ArrayBuffer[POCNode]],
       hashCount: HashMap[POCNode, Int],
       hashChildren: HashMap[(POCNode, Int), POCNode]): POCNode = {

    // create children HashMap if it does not exist
    var child = hashChildren.getOrElse((parent, item), null)
    if (child==null) {
      child = new POCNode(item, parent)

      if (headTable(item)==null) headTable(item) = new ArrayBuffer[POCNode]()
      headTable(item) += child

      hashCount.update(child, 1)
      hashChildren.update((parent, item), child)
    } else {
      hashCount.update(child, hashCount(child) + 1)
    }

    child
  }

  def genPrefixTree(iterator: Iterator[Array[Int]]) : Iterator[(POCNode, Array[ArrayBuffer[POCNode]], HashMap[POCNode, Int], Array[Array[Int]])] = {

    val tree = new POCNode()

    val headTable = new Array[ArrayBuffer[POCNode]](numF1Items)
    for(i <- 0 until numF1Items) headTable(i) = null

    val hashChildren = new HashMap[(POCNode, Int), POCNode]()
    val hashCount = new HashMap[POCNode, Int]()

    while (iterator.hasNext) { // process the partition to build a prefix tree (with prefixTree as root)
      val arr = iterator.next()

      var node = tree
      arr.foreach { item =>
        node = getOrCreateChild(item, node, headTable, hashCount, hashChildren)
      }
    }

    val f2Set = new Array[Array[Int]](numF1Items)

    Iterator((tree, headTable, hashCount, f2Set))
  }

  // get the potential frequent pattern ending with item i
  def getPotentialFPi(
       item: Int,
       postfix: String,
       f2Set: Array[Array[Int]],
       headTable: Array[ArrayBuffer[POCNode]],
       hashCount: HashMap[POCNode, Int],
       bcF1List: Broadcast[Array[(String, Int)]]): Array[(String, Int)] = {

    val nextHashCount = new HashMap[POCNode, Int]()
    val nextHeadTable = new Array[ArrayBuffer[POCNode]](item)

    val f2 = f2Set(item)
    val countFreq = new Array[Int](f2.length)

    // reverse traverse all paths ending by item to build up
    // new headTable and hashCount for next round of recursive calling
    headTable(item).foreach{ node =>
      val nodeCnt = hashCount(node)
      var i = f2.length - 1
      var curNode = node.parent
      while (i >= 0 && (curNode!=null && curNode.item>=0)) {
        val curItem = f2(i)
        if (curNode.item==curItem) {
          if(nextHeadTable(curItem)==null) nextHeadTable(curItem) = new ArrayBuffer[POCNode]()

          val cnt = nextHashCount.getOrElse(curNode, 0)
          if (cnt==0) nextHeadTable(curItem).prepend(curNode) // Yes it's a un-hashed node
          nextHashCount.update(curNode, cnt + nodeCnt)

          countFreq(i) += nodeCnt

          i -= 1
          curNode = curNode.parent
        } else {
          if (curNode.item > curItem) curNode = curNode.parent
          else i -= 1
        }
      }
    }

    countFreq.zipWithIndex
      .filter { case (cnt, indx) => cnt > 0 }
      .flatMap { case (cnt, indx) =>
        val newPostfix = bcF1List.value(f2(indx))._1 + splitterPattern + postfix
        val pFP = new Array[(String, Int)](1)
        pFP(0) = (newPostfix, cnt)
        if ((indx==0) || (f2Set(indx)==null) || (f2Set(indx).length==0)) pFP
        else {
          getPotentialFPi(f2(indx), newPostfix, f2Set, nextHeadTable, nextHashCount, bcF1List):+(newPostfix, cnt)
        }
      }
  }

  // calculate the frequent patterns ending with item i
  def calcFPi(
       item: Int,
       bcF1List: Broadcast[Array[(String, Int)]],
       forest: RDD[(POCNode, Array[ArrayBuffer[POCNode]], HashMap[POCNode, Int], Array[Array[Int]])]) : RDD[(String, Int)] = {

      val subForest =
        forest.filter{ case(tree, headTable, hashCount, f2Set) =>
          headTable(item) != null
        }
        .map{ case(tree, headTable, hashCount, f2Set) =>
          val nextHeadTable = new Array[ArrayBuffer[POCNode]](item)
          val nextHashCount = new mutable.HashMap[POCNode, Int]()
          val totalCount = new Array[Int](item)

          headTable(item).foreach{ node =>
            val nodeCnt = hashCount(node)
            var curNode = node.parent

            while (curNode!=null && curNode.item>=0) {
              if(nextHeadTable(curNode.item)==null) nextHeadTable(curNode.item) = new ArrayBuffer[POCNode]()

              val cnt = nextHashCount.getOrElse(curNode, 0)
              if (cnt==0) nextHeadTable(curNode.item).prepend(curNode) // Yes it's a un-hashed node
              nextHashCount.update(curNode, cnt + nodeCnt)

              totalCount(curNode.item) += nodeCnt
              curNode = curNode.parent
            }
          }
        (tree, nextHeadTable, nextHashCount, totalCount, f2Set)
      }

    val f2i =
      subForest.flatMap{ case(tree, nextHeadTable, nextHashCount, totalCount, f2Set) =>
        totalCount.zipWithIndex.map{ case(cnt, i) => (i, cnt) }
      }
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupport)

    // broadcast all items f where (item, f) are frequent
    val bcF2i = subForest.context.broadcast(f2i.map{ case(i, cnt) => i }.collect.sortWith(_ < _))

    subForest.flatMap { case (tree, nextHeadTable, nextHashCount, totalCount, f2Set) =>
      f2Set(item) = bcF2i.value
/*      
      var out = item + s"---\n"
      for(i <- 0 until f2Set.length) {
        out += i + s" -- " + (if(f2Set(i)==null) s"NULL" else f2Set(i).mkString(splitterPattern))
        out += s"\n"
      }
      println(out)
*/
      f2Set(item).filter{ i => (i > 0) && (totalCount(i) > 0) && (f2Set(i)!=null) && (f2Set(i).length>0)}.flatMap { i =>
        val postfix = bcF1List.value(i)._1 + splitterPattern + bcF1List.value(item)._1
        getPotentialFPi(i, postfix, f2Set, nextHeadTable, nextHashCount, bcF1List)
      }
    }
    .reduceByKey(_ + _)
    .filter(_._2 >= minSupport)
    .++(
      f2i.map{ case(i, cnt) =>
        (bcF1List.value(i)._1 + splitterPattern + bcF1List.value(item)._1, cnt)
      }
    )
  }

  /** Implementation of SyncFPGrowth. */
  def run(data: RDD[String]): RDD[(String, Int)] = {

    val sc = data.sparkContext
    val cdata = data.coalesce(SyncFPGrowth.DEFAULT_NUM_GROUPS).cache()

    // build f1list and f1map
    val f1List = calcF1Items(cdata).cache
    val bcF1List = sc.broadcast(f1List.collect().sortWith(_._2 > _._2))
    numF1Items = bcF1List.value.length
    println(s"f1List length = " + bcF1List.value.length + ", [" + bcF1List.value.mkString + "]")

    val f1Map = buildF1Map(bcF1List.value)
    val bcF1Map = sc.broadcast(f1Map)
    //    println(s"f1Map length = " + f1Map.size + ", [" + f1Map.toArray.mkString + "]")

    // mining frequent item sets
    val prefixForest = cdata.map { record =>
      record.split(splitterPattern)
        .filter { item => bcF1Map.value.contains(item)}
        .map(bcF1Map.value(_))
        .sortWith(_ < _)
    }
    .mapPartitions(genPrefixTree)
    .persist(StorageLevel.MEMORY_AND_DISK)

    cdata.unpersist()

    val fiSeq =
      for (i <- 1 until numF1Items) yield {
        val fi = calcFPi(i, bcF1List, prefixForest)
        fi.count
        fi
      }

    new UnionRDD(sc, fiSeq).++(f1List)
  }
}

/**
 * Top-level methods for calling SyncFPGrowth.
 */
object SyncFPGrowth {

  // Default values.
  val DEFAULT_SUPPORT_THRESHOLD = 0
  val DEFAULT_SPLITTER_PATTERN = " "
  val DEFAULT_NUM_GROUPS = 192

  /**
   * Run SyncFPGrowth using the given set of parameters.
   * @param data transactional dataset stored as `RDD[String]`
   * @param supportThreshold support threshold
   * @param splitterPattern splitter pattern
   * @return frequent itemsets stored as `RDD[(String, Int)]`
   */
  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String): RDD[(String, Int)] = {
    new SyncFPGrowth()
      .setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .run(data)
  }

  def run(data: RDD[String], supportThreshold: Double): RDD[(String, Int)] = {
    new SyncFPGrowth()
      .setSupportThreshold(supportThreshold)
      .run(data)
  }

  def run(data: RDD[String]): RDD[(String, Int)] = {
    new SyncFPGrowth()
      .run(data)
  }
}
