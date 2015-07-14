import java.{util => ju}

import org.apache.spark.{Partitioner, HashPartitioner, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by clin3 on 2015/6/4.
 */
class ParPrePostModel[Item: ClassTag](val freqItemsets: RDD[(Array[Item], Long)]) extends Serializable

class ParPrePost private(
    private var minSupport: Double,
    private var numPartitions: Int) extends Logging with Serializable {
  def this() = this(0.3, -1)

  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run[Item: ClassTag](data: RDD[Array[Item]]): ParPrePostModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    new ParPrePostModel(freqItemsets.persist())
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
    println("numFreqItems = " + numFreqItems)
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
      val tree = new PPCTree(numFreqItems, minCount, bcRankToItem.value)
      condTransactions.foreach{ transaction =>
        tree.add(transaction)
      }
      println("nodeCount = " + tree.PPCTreeNodeCount)
      tree.genNodeList(id, partitioner)
      val freqItemsets = tree.mine(bcRankToCount.value, id, partitioner)
      println("level1 time = " + tree.level1)
      println("initial time = " + tree.initial)
      println("nodelist count = " + tree.nodelistCount)
      println("bf count = " + tree.bfCount)
      freqItemsets
    }
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
