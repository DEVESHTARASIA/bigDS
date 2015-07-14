package org.apache.spark.armlib.fpm

import java.{util => ju}

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Logging, Partitioner}

import scala.collection.mutable
import scala.compat.Platform._
import scala.reflect.ClassTag

/**
 * :: Experimental ::
 *
 * Model trained by [[PFIN]], which holds frequent itemsets.
 * @param freqItemsets frequent itemset, which is an RDD of [[(Array[Item], Long)]]
 * @tparam Item item type
 */
@Experimental
class PFINModel[Item: ClassTag](val freqItemsets: RDD[(Array[Item], Long)]) extends Serializable

/**
 * :: Experimental ::
 *
 * @param minSupport the minimal support level of the frequent pattern, any pattern appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 * @param numPartitions number of partitions used by PFIN
 *
 * @see [[http://en.wikipedia.org/wiki/Association_rule_learning Association rule learning
 *       (Wikipedia)]]
 */
@Experimental
class PFIN private (
    private var minSupport: Double,
    private var numPartitions: Int) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
   * as the input data}.
   */
  def this() = this(0.3, -1)

  /**
   * Sets the minimal support level (default: `0.3`).
   */
  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  /**
   * Sets the number of partitions used by FPIN (default: same as input data).
   */
  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run[Item: ClassTag](data: RDD[Array[Item]]): PFINModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    new PFINModel(freqItemsets.persist())
  }

  private def genFreqItems[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      partitioner: Partitioner): Array[(Item, Long)] = {
      data.mapPartitions{ partition =>
      val mapItemCount = new mutable.HashMap[Item, Long]()
      partition.foreach{ transaction =>
        transaction.foreach{ item =>
          mapItemCount.update(item, mapItemCount.getOrElse(item, 0L) + 1)
        }
      }
      mapItemCount.iterator
    }
    .reduceByKey(partitioner, _ + _)
    .filter(_._2 >= minCount)
    .collect()
    .sortBy(-_._2)
  }

  private def genFreqItemsets[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      freqItems: Array[(Item, Long)],
      partitioner: Partitioner): RDD[(Array[Item], Long)] = {
    val numFreqItems = freqItems.length
    val itemToRank = freqItems.map(_._1).zipWithIndex.toMap
    val rankToItem = itemToRank.map(_.swap)
    val rankToCount = freqItems.map(_._2).zipWithIndex.map(_.swap)
    val bcItemToRank = data.context.broadcast(itemToRank)
    val bcRankToItem = data.context.broadcast(rankToItem)
    val bcRankToCount = data.context.broadcast(rankToCount)
    data.flatMap { transaction =>
      genCondTransactions(transaction, bcItemToRank.value, partitioner)
    }
    .aggregateByKey(mutable.ArrayBuffer.empty[Array[Int]], partitioner.numPartitions)(
      (buf, t) => buf += t,
      (buf1, buf2) => buf1 ++= buf2
    )
    .flatMap{ case(id, condTransactions) =>
/*      println("id = " + id)
      for(i <- condTransactions) {
        for(j <- i) {
          print(j + " ")
        }
        print("\r\n")
      }*/
      val tree = new POCTree(numFreqItems, minCount, bcRankToItem.value)
      condTransactions.foreach{ transaction =>
        tree.add(transaction)
      }
      tree.gen2Nodesets(id, partitioner)
      val freqItemsets = tree.mine(bcRankToCount.value, id, partitioner)
      freqItemsets
    }
    //.reduceByKey((u, v) => u, partitioner.numPartitions)
  }

  private def genCondTransactions[Item: ClassTag](
      transaction: Array[Item],
      itemToRank: Map[Item, Int],
      partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}

