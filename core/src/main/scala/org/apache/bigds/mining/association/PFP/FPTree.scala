package org.apache.bigds.mining.association.PFP

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * TreeNode.scala
 * Description: This is the definition of TreeNode of FP-Tree
 * Author: Lin, Chen
 * E-mail: chlin.ecnu@gmail.com
 * Version: 1.0
 */

/**
 * base node element of the FPTree
 * @param name
 * @param count
 * @param parent
 * @param children
 * @param nextHomonym
 */
class TreeNode (val name: String = null, var count: Int = 0, var parent: TreeNode = null, val children: ArrayBuffer[TreeNode] = new ArrayBuffer[TreeNode](),  var nextHomonym: TreeNode = null){
  def findChild(name: String): TreeNode = {
    children.find(_.name == name) match {
      case Some(node) => node
      case None => null
    }
  }
}


/**
 * Implementation of classical FPTree
 */
class FPTree() {

  var freqItemsets = new ArrayBuffer[(String, Int)]

  /** Build header table. */
  def buildHeaderTable(
      data: Iterable[ArrayBuffer[String]],
      minSupport: Double): ArrayBuffer[TreeNode] = {

    if (data.nonEmpty) {
      val map: HashMap[String, TreeNode] = new HashMap[String, TreeNode]()
      for (record <- data) {
        for (item <- record) {
          if (!map.contains(item)) {
            val node: TreeNode = new TreeNode(item)
            node.count = 1
            map(item) = node
          } else {
            map(item).count += 1
          }
        }
      }
      val headerTable = new ArrayBuffer[TreeNode]()
      map.filter(_._2.count >= minSupport).values.toArray
        .sortWith(_.count > _.count).copyToBuffer(headerTable)
      headerTable
    } else {
      null
    }
  }

  /** Build local FPTree on each node. */
  def buildFPTree(
                        data: Iterable[ArrayBuffer[String]],
                        headerTable: ArrayBuffer[TreeNode]): TreeNode = {
    val root: TreeNode = new TreeNode()
    for (record <- data) {
      val sortedTransaction = sortByHeaderTable(record, headerTable)
      var subTreeRoot: TreeNode = root
      var tmpRoot: TreeNode = null
      if (root.children.nonEmpty) {
        while (sortedTransaction.nonEmpty &&
          subTreeRoot.findChild(sortedTransaction.head.toString) != null) {
          tmpRoot = subTreeRoot.children.find(_.name.equals(sortedTransaction.head))
          match {
            case Some(node) => node
            case None => null
          }
          tmpRoot.count += 1
          subTreeRoot = tmpRoot
          sortedTransaction.remove(0)
        }
      }
      addNodes(subTreeRoot, sortedTransaction, headerTable)
    }

    /** Sort items in descending order of support. */
    def sortByHeaderTable(
                           transaction: ArrayBuffer[String],
                           headerTable: ArrayBuffer[TreeNode]): ArrayBuffer[String] = {
      val map: HashMap[String, Int] = new HashMap[String, Int]()
      for (item <- transaction) {
        for (index <- 0 until headerTable.length) {
          if (headerTable(index).name.equals(item)) {
            map(item) = index
          }
        }
      }

      val sortedTransaction: ArrayBuffer[String] = new ArrayBuffer[String]()
      map.toArray.sortWith(_._2 < _._2).foreach(sortedTransaction += _._1)
      sortedTransaction
    }

    /** Insert nodes into FPTree. */
    def addNodes(
                  parent: TreeNode,
                  transaction: ArrayBuffer[String],
                  headerTable: ArrayBuffer[TreeNode]) {
      while (transaction.nonEmpty) {
        val name: String = transaction.head
        transaction.remove(0)
        val leaf: TreeNode = new TreeNode(name)
        leaf.count = 1
        leaf.parent = parent
        parent.children += leaf

        var temp = true
        var index: Int = 0

        while (temp && index < headerTable.length) {
          var node = headerTable(index)
          if (node.name.equals(name)) {
            while(node.nextHomonym != null)
              node = node.nextHomonym
            node.nextHomonym  = leaf
            temp = false
          }
          index += 1
        }

        addNodes(leaf, transaction, headerTable)
      }
    }

    root
  }

  /** Implementation of classical FPGrowth. Mining frequent itemsets from FPTree. */
  def fpgrowth(
                transactions: Iterable[ArrayBuffer[String]],
                prefix: ArrayBuffer[String],
                minSupport: Double) {
    val headerTable: ArrayBuffer[TreeNode] = buildHeaderTable(transactions, minSupport)

    val treeRoot = buildFPTree(transactions, headerTable)

    if (treeRoot.children.nonEmpty) {
      if (prefix.nonEmpty) {
        for (node <- headerTable) {
          var tempStr: String = ""
          val tempArr = new ArrayBuffer[String]()
          tempArr += node.name
          for (pattern <- prefix) {
            tempArr += pattern.toString
          }
          tempStr += tempArr.sortWith(_ < _).mkString(" ").toString
          freqItemsets += tempStr -> node.count
        }
      }

      for (node: TreeNode <- headerTable) {
        val newPostPattern: ArrayBuffer[String] = new ArrayBuffer[String]()
        newPostPattern += node.name
        if (prefix.nonEmpty)
          newPostPattern ++= prefix
        val newTransactions: ArrayBuffer[ArrayBuffer[String]] =
          new ArrayBuffer[ArrayBuffer[String]]()
        var backNode: TreeNode = node.nextHomonym
        while (backNode != null) {
          var counter: Int = backNode.count
          val preNodes: ArrayBuffer[String] = new ArrayBuffer[String]()
          var parent: TreeNode = backNode.parent
          while (parent.name != null) {
            preNodes += parent.name
            parent = parent.parent
          }
          while (counter > 0) {
            newTransactions += preNodes
            counter -= 1
          }
          backNode = backNode.nextHomonym
        }

        fpgrowth(newTransactions, newPostPattern, minSupport)
      }

    }
  }
}
